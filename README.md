# Tube Inspection Integrator

This project provides an automated ETL pipeline that integrates data from a Faro Arm scanner, synchronizes it with an ERP system via SQL, and sends corrections or updates via socket to a microcontroller, which then communicates with industrial machines using the Modbus protocol.


## Architecture Overview

- **Faro Arm Scanner**  
  Captures dimensional data and generates JSON files through a macro-based script.

- **ERP System**  
  Receives and updates production data through SQL queries in a Microsoft Access database.

- **Industrial Machines**  
  Receive real-time updates and correction parameters via socket/Modbus connections.

## How it Works

1. The scanner generates a `.json` file after each tube inspection.
2. A file watcher detects the new file and parses its contents.
3. Extracted data is:
   - Sent to a remote API (For the new ERP system)
   - Used to update production data in the ERP
   - Broadcasted to connected machines using socket or Modbus with LRA corrections


## License

You are free to use, modify, and distribute this project as long as the original license information is retained.

## Contact

**Dieyson Ruthes**  
📧 [dieyson.ruthes@gmail.com](mailto:dieyson.ruthes@gmail.com)  
🔗 [LinkedIn Profile](https://www.linkedin.com/in/dieyson-ruthes-554376235/)

Feel free to reach out with questions, suggestions, or feedback!
