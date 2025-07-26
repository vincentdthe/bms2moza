it tricks Moza Cockpit into thinking that DCS is in execution and converts data from BMS shared memory into the format that Moza Cockpit expects from DCS World.
Compile with cargo build --release

and rename to DCS.exe

start the app, start moza cockpit and start BMS

compiled exe available in release.

inspiration and credits to
https://github.com/Bartosso/bms-to-ffbeast/tree/main


/// TODO ////

Improve the quantity and quality of data that can be taken from BMS shared memory and converted for use with what Moza Cockpit app expects
