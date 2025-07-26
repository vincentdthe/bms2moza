use bms_sm::*;
//use std::net::{TcpListener, TcpStream, SocketAddr}; //commented to disable the internal clent
use std::net::{TcpListener, TcpStream};
use std::{io::Write, thread, time};
use std::time::Duration;
use std::io::Read;
use tailcall::tailcall;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

const TICK_SLEEP_TIME: Duration = time::Duration::from_millis(10);
const WAITING_SIM_AND_TELEMETRY_SLEEP_TIME: Duration = time::Duration::from_millis(300);
const TCP_BIND_ADDRESS: &str = "127.0.0.1:1234";

fn main() {
    println!("[INFO] Initializing TCP server on {}", TCP_BIND_ADDRESS);
    let listener = TcpListener::bind(TCP_BIND_ADDRESS).expect("[ERROR] Cannot bind to TCP port");

    println!("[INFO] Waiting for BMS to start...");
    let flight_data = Arc::new(wait_for_flight_data());
    let intellivibe_data = Arc::new(wait_for_intellivibe_data());     
    println!("[INFO] BMS memory mapped. Starting TCP loop...");

    // Shared state for managing clients
    let clients: Arc<Mutex<HashMap<String, TcpStream>>> = Arc::new(Mutex::new(HashMap::new()));
    let should_stop = Arc::new(Mutex::new(false));

    // Data generation thread - mimics the LuaExportAfterNextFrame behavior
    let clients_clone = Arc::clone(&clients);
    let flight_data_clone = Arc::clone(&flight_data);
    let intellivibe_data_clone = Arc::clone(&intellivibe_data);
    let should_stop_clone = Arc::clone(&should_stop);

    let data_thread = thread::spawn(move || {
        data_sender_loop(clients_clone, flight_data_clone, intellivibe_data_clone, should_stop_clone);
    });

/*
//// INTERNAL CLIENT DEBUGGER: Disabled for production use

// let clients_internal = Arc::clone(&clients);
// let internal_addr_marker = Arc::new(Mutex::new(None::<SocketAddr>));
// let internal_addr_marker_clone = Arc::clone(&internal_addr_marker);

// thread::spawn(move || {
//     thread::sleep(Duration::from_millis(500)); // Wait for server to be ready
//     println!("[DEBUG] Internal client attempting to connect to server...");
//     match TcpStream::connect(TCP_BIND_ADDRESS) {
//         Ok(stream) => {
//             println!("[DEBUG] Internal client connected to server.");
//             stream.set_nonblocking(true).expect("Failed to set non-blocking");
//             let peer = stream.peer_addr().unwrap();
//             {
//                 let mut clients_map = clients_internal.lock().unwrap();
//                 clients_map.insert(format!("internal_{}", peer), stream);
//             }
//             *internal_addr_marker_clone.lock().unwrap() = Some(peer);
//             println!("[DEBUG] Internal client registered");
//         }
//         Err(e) => println!("[ERROR] Internal client connection failed: {}", e),
//     }
// });
*/

//commented to disable compilation warning after commenting internal client code
//let internal_addr_marker = Arc::new(Mutex::new(None::<SocketAddr>)); // Keep this dummy so .lock() doesn't panic

    // Accept external connections (like MOZA Cockpit)
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let peer_addr = stream.peer_addr().unwrap();

                // Commented out internal client check
                // Skip registering the internal client again
                // if let Some(internal_addr) = *internal_addr_marker.lock().unwrap() {
                        //     if peer_addr == internal_addr {
                //         continue;
                //     }
                // }

                println!("[INFO] External client connected: {}", peer_addr);
                stream.set_nonblocking(true).expect("Failed to set non-blocking");

                {
                    let mut clients_map = clients.lock().unwrap();
                    clients_map.insert(format!("external_{}", peer_addr), stream);
                }

                println!("[DEBUG] External client registered: {}", peer_addr);
            }
            Err(e) => println!("[ERROR] Connection failed: {}", e),
        }
    }

    // Wait for data thread to complete to avoid exiting immediately
    let _ = data_thread.join();

    //fn compute_is_on_ground(intellivibe: &IntellivibeData) -> bool {
    //intellivibe.on_ground
    //}

}
fn data_sender_loop(
    clients: Arc<Mutex<HashMap<String, TcpStream>>>,
    flight_data: Arc<MemoryFile<'static, FlightData>>,
    intellivibe_data: Arc<MemoryFile<'static, IntellivibeData>>,
    should_stop: Arc<Mutex<bool>>,
) {
    loop {
        thread::sleep(TICK_SLEEP_TIME);

        let intellivibe_data_current = intellivibe_data.read();
        if intellivibe_data_current.exit_game {
            println!("[INFO] Exit flag detected. Sending stop signal and terminating.");
            
            // Send stop signal to all clients
            let stop_msg = "export_stop,true;";
            {
                let mut clients_map = clients.lock().unwrap();
                clients_map.retain(|id, stream| {
                    match stream.write_all(stop_msg.as_bytes()) {
                        Ok(_) => {
                            let _ = stream.flush();
                            println!("[DEBUG] Sent stop signal to client: {}", id);
                            false // Remove client after sending stop signal
                        }
                        Err(e) => {
                            println!("[ERROR] Failed to send stop signal to {}: {}", id, e);
                            false // Remove failed client
                        }
                    }
                });
            }
            
            *should_stop.lock().unwrap() = true;
            break;
        }

        let flight_data_current = flight_data.read();

        let output = if intellivibe_data_current.paused
            || intellivibe_data_current.ejecting
            || intellivibe_data_current.end_flight
        {
            println!("[DEBUG] Paused/Ejecting/EndFlight detected, sending zero data.");
            //compute_zero_data() //commented to try to sort the vibration issue after exiting and re-entering mission
            continue; // skip sending anything - addedd to address what described in the above comment

        } else {
            let msg = compute_actual_flight_data(&flight_data_current, &intellivibe_data_current);
            println!("[DEBUG] Sending data: {}", msg.chars().take(100).collect::<String>() + "...");
            msg
        };

        // Send data to all connected clients
        {
            let mut clients_map = clients.lock().unwrap();
            clients_map.retain(|id, stream| {
                // First, try to read any incoming data (like the Lua script does)
                let mut buffer = [0; 512];
                match stream.read(&mut buffer) {
                    Ok(0) => {
                        println!("[INFO] Client {} disconnected", id);
                        return false; // Remove disconnected client
                    }
                    Ok(size) => {
                        println!("[DEBUG] Received {} bytes from client {}", size, id);
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // No data available, continue
                    }
                    Err(e) => {
                        println!("[ERROR] Client {} read error: {}", id, e);
                        return false; // Remove failed client
                    }
                }

                // Send telemetry data
                match stream.write_all(output.as_bytes()) {
                    Ok(_) => {
                        if let Err(e) = stream.flush() {
                            println!("[WARN] Failed to flush stream for {}: {}", id, e);
                        }
                        true // Keep client
                    }
                    Err(e) => {
                        println!("[ERROR] Failed to write to {}: {}", id, e);
                        false // Remove failed client
                    }
                }
            });
        }
    }
}

#[tailcall]
fn wait_for_flight_data() -> MemoryFile<'static, FlightData> {
    thread::sleep(WAITING_SIM_AND_TELEMETRY_SLEEP_TIME);
    match FlightData::new() {
        Ok(data) => {
            println!("[INFO] FlightData memory mapped successfully.");
            data
        },
        Err(e) => {
            println!("[WARN] FlightData not yet available. Error: {:?}. Retrying...", e);
            wait_for_flight_data()
        },
    }
}

#[tailcall]
fn wait_for_intellivibe_data() -> MemoryFile<'static, IntellivibeData> {
    thread::sleep(WAITING_SIM_AND_TELEMETRY_SLEEP_TIME);
    match IntellivibeData::new() {
        Ok(data) => {
            println!("[INFO] IntellivibeData memory mapped successfully.");
            data
        },
        Err(e) => {
            println!("[WARN] IntellivibeData not yet available. Error: {:?}. Retrying...", e);
            wait_for_intellivibe_data()
        },
    }
}

fn compute_actual_flight_data(
    flight_data: &FlightData,
    _intellivibe_data: &IntellivibeData,
) -> String {
    // Convert BMS data to MOZA format (key,value;key,value;...)
    let mut data = String::new();
    
    // Aircraft name - simulate F-16C for MOZA
    data.push_str("aircraft_name,F-16C_50;");
    
    // Engine RPM (BMS has single engine, duplicate for left/right)
    let engine_rpm = flight_data.rpm;
    data.push_str(&format!("engine_rpm_left,{:.2};", engine_rpm));
    data.push_str(&format!("engine_rpm_right,{:.2};", engine_rpm));

    
    // Gear (from BMS gear_pos: 0.0 = up, 1.0 = down)
    data.push_str("gearSuccess,true;");
    data.push_str(&format!("gear_value,{:.2};", flight_data.gear_pos));
    
    // Acceleration (BMS provides Gs, convert to m/s²)
    let acc_x = flight_data.gs * 9.81; // Assuming longitudinal G-force
    data.push_str(&format!("acc_x,{:.2};", acc_x));
    data.push_str("acc_y,0.00;"); // BMS doesn't provide lateral G directly
    data.push_str(&format!("acc_z,{:.2};", flight_data.gs * 9.81));
    
    // Wind (BMS doesn't provide wind data directly)
    data.push_str("wind_x,0.00;wind_y,0.00;wind_z,0.00;");
    
    // Velocity (convert from BMS data)
    // BMS provides velocity in various forms, using what's available
    data.push_str(&format!("vector_velocity_x,{:.2};", flight_data.x_dot * 0.3048)); // ft/s to m/s
    data.push_str(&format!("vector_velocity_y,{:.2};", flight_data.y_dot * 0.3048));
    data.push_str(&format!("vector_velocity_z,{:.2};", flight_data.z_dot * 0.3048));
    
    // Airspeeds (using the correct field names from BMS)
    let tas = flight_data.kias * 0.514444; // knots to m/s
    let ias = flight_data.kias * 0.514444; // knots to m/s
    data.push_str(&format!("tas,{:.2};", tas));
    data.push_str(&format!("ias,{:.2};", ias));
    data.push_str(&format!("vertical_velocity_speed,{:.2};", -flight_data.z_dot * 0.3048));
    
    // Angles (using correct BMS field names)
    data.push_str(&format!("aoa,{:.2};", flight_data.alpha)); // AOA
    data.push_str(&format!("heading,{:.2};", flight_data.yaw)); // Heading 
    data.push_str(&format!("pitch,{:.2};", flight_data.pitch)); // Pitch
    data.push_str(&format!("bank,{:.2};", flight_data.roll)); // Roll/Bank
    
    // Side slip angle (beta)
    data.push_str(&format!("aos,{:.2};", flight_data.beta));
    
    // Angular velocities (BMS doesn't provide direct angular velocity, use approximation or zero)
    data.push_str("euler_vx,0.00;"); // Roll rate not directly available
    data.push_str("euler_vy,0.00;"); // Pitch rate not directly available  
    data.push_str("euler_vz,0.00;"); // Yaw rate not directly available
    
    // Main rotor RPM (not applicable for F-16, set to false)
    data.push_str("mainRotorRPMSuccess,false;");
    
    // Mechanical info
    data.push_str("canopy_pos,0.0;"); // BMS doesn't provide canopy pos directly
    data.push_str("flap_pos,0.0;"); // F-16 doesn't have traditional flaps
    data.push_str(&format!("speedbrake_value,{:.2};", flight_data.speed_brake));
    
    // Afterburner (BMS might have ftit or other engine parameters for this)
    // The reference code uses flight_data.rpm for thrust
    // You may need to find the right field for afterburner detection
    let afterburner = if flight_data.rpm > 0.8 { 1.0 } else { 0.0 };
    // Alternative approaches:
    // - Check if there's a ftit (fan turbine inlet temperature) field
    // - Check if there's a nozzle position field
    // - Use engine RPM threshold as approximation
    data.push_str(&format!("afterburner_1,{:.2};", afterburner));
    data.push_str(&format!("afterburner_2,{:.2};", afterburner)); // F-16 has single engine
    
    // Weapon system
    data.push_str("weaponSuccess,true;"); // Assume weapons available
    
    // Spoilers (F-16 doesn't have spoilers like airliners)
    data.push_str("spoilerSuccess,false;");
    
    // Mach number
    data.push_str(&format!("mach,{:.2};", flight_data.mach));
    
    // Altitude (using z coordinate which is typically altitude in BMS)
    let altitude_m = flight_data.z * 0.3048; // feet to meters
    data.push_str(&format!("h_above_sea_level,{:.2};", altitude_m));
    
    // Panel lights
    data.push_str("panelLightSuccess,false;");
    
    data
}

fn compute_zero_data() -> String {
    // Send zero data in MOZA format when paused/ejecting
    let mut data = String::new();
    data.push_str("aircraft_name,F-16C_50;");
    data.push_str("engine_rpm_left,0.00;engine_rpm_right,0.00;");
    data.push_str("gearSuccess,true;gear_value,1.00;"); // Gear down when stopped
    data.push_str("acc_x,0.00;acc_y,0.00;acc_z,9.81;"); // Just gravity
    data.push_str("wind_x,0.00;wind_y,0.00;wind_z,0.00;");
    data.push_str("vector_velocity_x,0.00;vector_velocity_y,0.00;vector_velocity_z,0.00;");
    data.push_str("tas,0.00;ias,0.00;vertical_velocity_speed,0.00;");
    data.push_str("aoa,0.00;heading,0.00;pitch,0.00;bank,0.00;");
    data.push_str("aos,0.00;euler_vx,0.00;euler_vy,0.00;euler_vz,0.00;");
    data.push_str("mainRotorRPMSuccess,false;");
    data.push_str("canopy_pos,0.0;flap_pos,0.0;speedbrake_value,0.00;");
    data.push_str("afterburner_1,0.00;afterburner_2,0.00;");
    data.push_str("weaponSuccess,true;spoilerSuccess,false;");
    data.push_str("mach,0.00;h_above_sea_level,0.00;");
    data.push_str("panelLightSuccess,false;");
    data
}