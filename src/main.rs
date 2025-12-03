
use bms_sm::*;
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
    listener.set_nonblocking(true).expect("[ERROR] Failed to set listener to non-blocking");

    let clients: Arc<Mutex<HashMap<String, TcpStream>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        // Clean up disconnected clients before restarting telemetry
        clients.lock().unwrap().retain(|_, stream| stream.peer_addr().is_ok());

        println!("[INFO] Waiting for BMS to start...");
        let flight_data = Arc::new(wait_for_flight_data());
        let intellivibe_data = Arc::new(wait_for_intellivibe_data());
        
        // StringData is accessed statically, so we don't map it here persistently.

        println!("[INFO] BMS memory mapped. Starting telemetry loop...");

        let should_stop = Arc::new(Mutex::new(false));

        let clients_clone = Arc::clone(&clients);
        let flight_data_clone = Arc::clone(&flight_data);
        let intellivibe_data_clone = Arc::clone(&intellivibe_data);
        let should_stop_clone = Arc::clone(&should_stop);

        let data_thread = thread::spawn(move || {
            data_sender_loop(
                clients_clone, 
                flight_data_clone, 
                intellivibe_data_clone, 
                should_stop_clone
            );
        });

        // Accept external connections
        loop {
            match listener.accept() {
                Ok((stream, addr)) => {
                    stream.set_nonblocking(true).expect("Failed to set non-blocking");
                    println!("[INFO] External client connected: {}", addr);
                    clients.lock().unwrap().insert(format!("external_{}", addr), stream);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if *should_stop.lock().unwrap() {
                        break;
                    }
                    thread::sleep(Duration::from_millis(100));
                }
                Err(e) => {
                    println!("[ERROR] Failed to accept connection: {}", e);
                    break;
                }
            }
        }

        let _ = data_thread.join();

        if !*should_stop.lock().unwrap() {
            break;
        }

        println!("[INFO] BMS restarted and memory remapped."); 
    }
}

fn data_sender_loop(
    clients: Arc<Mutex<HashMap<String, TcpStream>>>,
    flight_data: Arc<MemoryFile<'static, FlightData>>,
    intellivibe_data: Arc<MemoryFile<'static, IntellivibeData>>,
    should_stop: Arc<Mutex<bool>>,
) {
    let mut tick_count: u64 = 0;
    
    // Default fallback values
    let mut current_ac_name = "F-16C_50".to_string();
    let mut _is_single_engine = true; 

    loop {
        thread::sleep(TICK_SLEEP_TIME);
        tick_count += 1;

        // Update string data periodically (every 1 second approx)
        if tick_count % 100 == 0 {
            // StringData::read() is a static method returning Result<HashMap<StringId, String>, ...>
            if let Ok(strings_map) = StringData::read() {
                if let Some(bms_name) = strings_map.get(&StringId::AcName) {
                    if bms_name.contains("F-15") {
                         current_ac_name = "F-15C".to_string();
                         _is_single_engine = false; 
                    } else if bms_name.contains("F/A-18") {
                         current_ac_name = "F/A-18C".to_string();
                         _is_single_engine = false; 
                    } else {
                         current_ac_name = "F-16C_50".to_string();
                         _is_single_engine = true; 
                    }
                }
            }
        }

        let intellivibe_data_current = intellivibe_data.read();
        if intellivibe_data_current.exit_game {
            println!("[INFO] Exit flag detected. Sending stop signal and terminating.");
            
            let stop_msg = "export_stop,true;";
            {
                let mut clients_map = clients.lock().unwrap();
                clients_map.retain(|id, stream| {
                    match stream.write_all(stop_msg.as_bytes()) {
                        Ok(_) => {
                            let _ = stream.flush();
                            println!("[DEBUG] Sent stop signal to client: {}", id);
                            false 
                        }
                        Err(e) => {
                            println!("[ERROR] Failed to send stop signal to {}: {}", id, e);
                            false 
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
            // Use defaults/zeros when paused
            compute_zero_data(&current_ac_name) 
        } else {
            compute_actual_flight_data(
                &flight_data_current, 
                &intellivibe_data_current, 
                &current_ac_name,
            )
        };

        // Send data to all connected clients
        {
            let mut clients_map = clients.lock().unwrap();
            clients_map.retain(|id, stream| {
                let mut buffer = [0; 512];
                match stream.read(&mut buffer) {
                    Ok(0) => {
                        println!("[INFO] Client {} disconnected", id);
                        return false; 
                    }
                    Ok(_) => {} // Consume input
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) => {
                        println!("[ERROR] Client {} read error: {}", id, e);
                        return false; 
                    }
                }

                match stream.write_all(output.as_bytes()) {
                    Ok(_) => {
                        if let Err(e) = stream.flush() {
                            println!("[WARN] Failed to flush stream for {}: {}", id, e);
                        }
                        true 
                    }
                    Err(e) => {
                        println!("[ERROR] Failed to write to {}: {}", id, e);
                        false 
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
        Err(_) => wait_for_flight_data(),
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
        Err(_) => wait_for_intellivibe_data(),
    }
}

fn compute_actual_flight_data(
    flight_data: &FlightData,
    _intellivibe_data: &IntellivibeData,
    ac_name: &str,
) -> String {
    let mut data = String::new();
    
    data.push_str(&format!("aircraft_name,{};", ac_name));
    
    // Engine RPM
    let rpm = flight_data.rpm;
    data.push_str(&format!("engine_rpm_left,{:.2};", rpm));
    data.push_str(&format!("engine_rpm_right,{:.2};", rpm)); 
    
    // Gear
    data.push_str("gearSuccess,true;");
    data.push_str(&format!("gear_value,{:.2};", flight_data.gear_pos));
    
    // Acceleration
    let acc_x = flight_data.gs * 9.81; 
    data.push_str(&format!("acc_x,{:.2};", acc_x));
    data.push_str("acc_y,0.00;"); 
    data.push_str(&format!("acc_z,{:.2};", flight_data.gs * 9.81));
    
    // Wind
    data.push_str("wind_x,0.00;wind_y,0.00;wind_z,0.00;");
    
    // Velocity
    data.push_str(&format!("vector_velocity_x,{:.2};", flight_data.x_dot * 0.3048)); 
    data.push_str(&format!("vector_velocity_y,{:.2};", flight_data.y_dot * 0.3048));
    data.push_str(&format!("vector_velocity_z,{:.2};", flight_data.z_dot * 0.3048));
    
    // Airspeeds
    let tas = flight_data.kias * 0.514444; 
    let ias = flight_data.kias * 0.514444; 
    data.push_str(&format!("tas,{:.2};", tas));
    data.push_str(&format!("ias,{:.2};", ias));
    data.push_str(&format!("vertical_velocity_speed,{:.2};", -flight_data.z_dot * 0.3048));
    
    // Angles
    data.push_str(&format!("aoa,{:.2};", flight_data.alpha)); 
    data.push_str(&format!("heading,{:.2};", flight_data.yaw)); 
    data.push_str(&format!("pitch,{:.2};", flight_data.pitch)); 
    data.push_str(&format!("bank,{:.2};", flight_data.roll)); 
    
    // Side slip
    data.push_str(&format!("aos,{:.2};", flight_data.beta));
    
    // Angular velocities
    data.push_str("euler_vx,0.00;"); 
    data.push_str("euler_vy,0.00;"); 
    data.push_str("euler_vz,0.00;"); 
    
    // Main rotor
    data.push_str("mainRotorRPMSuccess,false;");
    
    // Mechanical info
    data.push_str("canopy_pos,0.0;"); 
    data.push_str("flap_pos,0.0;"); 
    data.push_str(&format!("speedbrake_value,{:.2};", flight_data.speed_brake));
    
    // Afterburner
    let afterburner = if flight_data.rpm > 0.95 { 1.0 } else { 0.0 }; 
    data.push_str(&format!("afterburner_1,{:.2};", afterburner));
    data.push_str(&format!("afterburner_2,{:.2};", afterburner)); 
    
    // Weapon system
    data.push_str("weaponSuccess,true;"); 
    
    // Spoilers
    data.push_str("spoilerSuccess,false;");
    
    // Mach number
    data.push_str(&format!("mach,{:.2};", flight_data.mach));
    
    // Altitude
    let altitude_m = -flight_data.z * 0.3048; 
    data.push_str(&format!("h_above_sea_level,{:.2};", altitude_m));
    
    // Panel lights
    data.push_str("panelLightSuccess,false;");
    
    data
}

fn compute_zero_data(ac_name: &str) -> String {
    let mut data = String::new();
    data.push_str(&format!("aircraft_name,{};", ac_name));
    data.push_str("engine_rpm_left,0.00;engine_rpm_right,0.00;");
    data.push_str("gearSuccess,true;gear_value,0.00;");
    data.push_str("acc_x,0.00;acc_y,0.00;acc_z,0;");
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