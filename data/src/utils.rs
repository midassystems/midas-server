use crate::error::Result;
use std::io::Write;

pub fn user_input() -> Result<bool> {
    // Check if running in a non-interactive mode (like in a cron job)
    if std::env::var("NON_INTERACTIVE").is_ok() {
        println!("Non-interactive mode detected. Proceeding with default behavior.");
        return Ok(true); // Default to proceeding or canceling based on your requirement
    }

    let mut attempts = 0; // Initialize a counter for attempts

    loop {
        // Prompt the user for input
        print!("Do you want to proceed? (y/n): ");
        std::io::stdout().flush()?; // Ensure the prompt is printed immediately

        // Read user input
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;

        // Trim and check the input
        match input.trim() {
            "y" | "Y" => {
                println!("Proceeding with the operation...");
                return Ok(true); // Return true to indicate confirmation
            }
            "n" | "N" => {
                println!("Operation canceled by the user.");
                return Ok(false); // Return false to indicate cancellation
            }
            _ => {
                println!("Invalid input. Please enter 'y' or 'n'.");
                attempts += 1; // Increment the counter for invalid input

                if attempts >= 3 {
                    println!("Too many invalid attempts. Defaulting to cancellation.");
                    return Ok(false); // Return false after 3 invalid attempts
                }
            }
        }
    }
}
