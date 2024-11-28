import streamlit as st
import subprocess
import sys
from datetime import datetime

# Set up Streamlit app
st.set_page_config(page_title="NBA Update Monitor", layout="wide")

# Sidebar navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to:", ["Trigger Updates", "View Update History"])

# Determine the correct Python executable
python_executable = sys.executable  # Dynamically detects the current Python executable

# Function to execute a script
def run_script(script_name):
    """Runs a Python script and logs the result."""
    try:
        # Execute the script and capture output
        result = subprocess.run([python_executable, script_name], text=True, capture_output=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Log success or failure
        if result.returncode == 0:
            log_entry = f"{timestamp} - {script_name} executed successfully.\n"
            st.success(f"{script_name} executed successfully.")
        else:
            log_entry = f"{timestamp} - {script_name} failed with error:\n{result.stderr}\n"
            st.error(f"{script_name} encountered an error.")
            st.text(result.stderr)

        # Write the log entry to a file
        with open("update_history.log", "a") as log_file:
            log_file.write(log_entry)

        # Optionally display script output
        st.text(result.stdout)
    except Exception as e:
        st.error(f"Failed to execute {script_name}: {e}")
        with open("update_history.log", "a") as log_file:
            log_file.write(f"{datetime.now()} - {script_name} failed with exception: {e}\n")

# Page 1: Trigger Updates
if page == "Trigger Updates":
    st.title("Trigger Updates")
    st.write("Manually trigger updates for prop markets or game logs.")

    # Button to run props.py
    if st.button("Run props.py (Update Prop Markets)"):
        st.write("Executing props.py...")
        run_script("props.py")

    # Button to run logs.py
    if st.button("Run logs.py (Update Game Logs)"):
        st.write("Executing logs.py...")
        run_script("logs.py")

# Page 2: View Update History
elif page == "View Update History":
    st.title("Update History")
    st.write("View the history of triggered updates.")

    try:
        # Read and display the log file
        with open("update_history.log", "r") as log_file:
            logs = log_file.readlines()
            st.text_area("Update History", value="".join(logs), height=400)
    except FileNotFoundError:
        st.warning("No update history found. Logs will appear here after updates are triggered.")

