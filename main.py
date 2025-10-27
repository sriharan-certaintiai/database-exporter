import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import mysql.connector
from mysql.connector import Error
import re
import configparser
import threading
import queue
import os
import json
from datetime import datetime
import webbrowser  # <-- IMPORT ADDED HERE

# --- Configuration and History File constants ---
CONFIG_FILE = 'config.ini'
HISTORY_FILE = 'export_history.json'
ALLOWED_QUERY = "SELECT * FROM consolidated_summary;"  # The only query allowed to run


class DbExporterApp:
    """
    A GUI application for exporting database queries to Excel.
    """

    def __init__(self, root):
        self.root = root
        self.root.title("1.0 Database Exporter | Certainti.Ai")
        self.root.geometry("600x480")  # Increased height for new settings
        self.root.minsize(500, 450)

        # --- App state variables ---
        self.config = configparser.ConfigParser()
        self.connections = {}  # Stores loaded connection details {platform_name: details_dict}
        self.platform_to_section = {}  # Maps platform_name to config section (e.g., 'conn1')
        self.current_settings_section = None  # Tracks which section is being edited
        self.history_data = []  # Stores history records
        self.export_queue = queue.Queue()  # Queue for thread communication

        # --- Styling ---
        self.style = ttk.Style()
        self.style.theme_use('clam')  # You can try 'default', 'alt', 'vista', 'xpnative'
        self.style.configure('TButton', padding=6, relief="flat", font=('Helvetica', 10, 'bold'))
        self.style.configure('TLabel', font=('Helvetica', 10))
        self.style.configure('TCombobox', font=('Helvetica', 10))
        self.style.configure('Treeview.Heading', font=('Helvetica', 10, 'bold'))
        self.style.configure('TEntry', font=('Helvetica', 10))
        self.style.configure('TLabelframe.Label', font=('Helvetica', 10, 'bold'))

        # --- Main UI Structure ---
        self.notebook = ttk.Notebook(root)

        # --- Tab 1: Export ---
        self.export_frame = ttk.Frame(self.notebook, padding="20")
        self.notebook.add(self.export_frame, text='Export')
        self.create_export_ui()

        # --- Tab 2: History ---
        self.history_frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(self.history_frame, text='History')
        self.create_history_ui()

        # --- Tab 3: Settings ---
        self.settings_frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(self.settings_frame, text='Settings')
        self.create_settings_ui()  # This is the refactored settings UI

        # --- Tab 4: About ---
        self.about_frame = ttk.Frame(self.notebook, padding="20")
        self.notebook.add(self.about_frame, text='About')
        self.create_about_ui()  # <-- THIS METHOD IS UPDATED

        self.notebook.pack(expand=True, fill='both')

        # --- Load initial data ---
        self.load_config()
        self.load_history()
        self.load_selected_conn_to_form()  # Load first item into settings form

        # --- Start queue checker ---
        self.root.after(100, self.check_queue)

    def create_export_ui(self):
        """Creates all widgets for the Export tab."""

        # --- Platform Selection ---
        platform_frame = ttk.Frame(self.export_frame)
        platform_frame.pack(fill='x', pady=(0, 20))

        lbl_platform = ttk.Label(platform_frame, text="Choose Platform:")
        lbl_platform.pack(side=tk.LEFT, padx=(0, 10))

        self.platform_combo = ttk.Combobox(platform_frame, state='readonly', width=30)
        self.platform_combo.pack(side=tk.LEFT, fill='x', expand=True)

        # --- Export Button ---
        self.export_button = ttk.Button(
            self.export_frame,
            text="Start Export...",
            command=self.start_export_thread
        )
        self.export_button.pack(pady=20, fill='x')

        # --- Status & Feedback Area ---
        status_group = ttk.LabelFrame(self.export_frame, text="Progress", padding=10)
        status_group.pack(fill='both', expand=True)

        self.progress_bar = ttk.Progressbar(
            status_group,
            mode='indeterminate'
        )
        self.progress_bar.pack(fill='x', pady=(5, 10))

        # Use a Text widget for scrolling status messages
        self.status_text = tk.Text(
            status_group,
            height=10,
            wrap=tk.WORD,
            font=('Courier New', 9),
            bg="#f0f0f0",
            borderwidth=1,
            relief="solid"
        )
        self.status_text.pack(fill='both', expand=True)

        # Define text tags for coloring
        self.status_text.tag_configure("info", foreground="black")
        self.status_text.tag_configure("error", foreground="red", font=('Courier New', 9, 'bold'))
        self.status_text.tag_configure("success", foreground="green", font=('Courier New', 9, 'bold'))

        self.update_status("Welcome! Please select a platform and start the export.")
        self.status_text.config(state=tk.DISABLED)  # Make it read-only

    def create_history_ui(self):
        """Creates all widgets for the History tab."""

        # --- Treeview for History ---
        cols = ('Platform', 'Date/Time', 'File Name', 'File Path')
        self.history_tree = ttk.Treeview(self.history_frame, columns=cols, show='headings')

        for col in cols:
            self.history_tree.heading(col, text=col)
            self.history_tree.column(col, width=120, anchor=tk.W)

        # --- Scrollbars for Treeview ---
        v_scroll = ttk.Scrollbar(self.history_frame, orient="vertical", command=self.history_tree.yview)
        h_scroll = ttk.Scrollbar(self.history_frame, orient="horizontal", command=self.history_tree.xview)
        self.history_tree.configure(yscrollcommand=v_scroll.set, xscrollcommand=h_scroll.set)

        h_scroll.pack(side=tk.BOTTOM, fill='x')
        v_scroll.pack(side=tk.RIGHT, fill='y')
        self.history_tree.pack(fill='both', expand=True)

        # --- Refresh Button ---
        refresh_button = ttk.Button(
            self.history_frame,
            text="Refresh History",
            command=self.load_history
        )
        refresh_button.pack(side=tk.BOTTOM, fill='x', pady=(10, 0))

    def create_settings_ui(self):
        """Creates the new structured settings tab."""

        # --- Top frame for selection ---
        selection_frame = ttk.Frame(self.settings_frame)
        selection_frame.pack(fill='x', pady=(0, 10))

        lbl_select = ttk.Label(selection_frame, text="Select Connection:")
        lbl_select.pack(side=tk.LEFT, padx=(0, 10))

        self.settings_conn_combo = ttk.Combobox(selection_frame, state='readonly')
        self.settings_conn_combo.pack(side=tk.LEFT, fill='x', expand=True)
        self.settings_conn_combo.bind("<<ComboboxSelected>>", self.load_selected_conn_to_form)

        # --- Main form for details ---
        form_frame = ttk.LabelFrame(self.settings_frame, text="Connection Details", padding=15)
        form_frame.pack(fill='both', expand=True)

        # Grid layout for the form
        form_frame.columnconfigure(1, weight=1)

        # --- StringVars to link to entries ---
        self.settings_name_var = tk.StringVar()
        self.settings_host_var = tk.StringVar()
        self.settings_db_var = tk.StringVar()
        self.settings_user_var = tk.StringVar()
        self.settings_pass_var = tk.StringVar()
        self.settings_query_var = tk.StringVar()

        # --- Form fields ---
        ttk.Label(form_frame, text="Name:").grid(row=0, column=0, sticky=tk.W, pady=5)
        ttk.Entry(form_frame, textvariable=self.settings_name_var).grid(row=0, column=1, sticky=tk.EW, pady=5)

        ttk.Label(form_frame, text="Host:").grid(row=1, column=0, sticky=tk.W, pady=5)
        ttk.Entry(form_frame, textvariable=self.settings_host_var).grid(row=1, column=1, sticky=tk.EW, pady=5)

        ttk.Label(form_frame, text="Database:").grid(row=2, column=0, sticky=tk.W, pady=5)
        ttk.Entry(form_frame, textvariable=self.settings_db_var).grid(row=2, column=1, sticky=tk.EW, pady=5)

        ttk.Label(form_frame, text="User:").grid(row=3, column=0, sticky=tk.W, pady=5)
        ttk.Entry(form_frame, textvariable=self.settings_user_var).grid(row=3, column=1, sticky=tk.EW, pady=5)

        ttk.Label(form_frame, text="Password:").grid(row=4, column=0, sticky=tk.W, pady=5)
        ttk.Entry(form_frame, textvariable=self.settings_pass_var, show="*").grid(row=4, column=1, sticky=tk.EW, pady=5)

        # --- READ-ONLY Query field ---
        ttk.Label(form_frame, text="Query:").grid(row=5, column=0, sticky=tk.W, pady=5)
        query_entry = ttk.Entry(form_frame, textvariable=self.settings_query_var, state='disabled')
        query_entry.grid(row=5, column=1, sticky=tk.EW, pady=5)

        # --- Button Frame ---
        button_frame = ttk.Frame(self.settings_frame)
        button_frame.pack(side=tk.BOTTOM, fill='x', pady=(10, 0))

        save_button = ttk.Button(
            button_frame,
            text="Save Changes",
            command=self.save_connection_changes
        )
        save_button.pack(side=tk.LEFT, fill='x', expand=True, padx=(0, 5))

        add_button = ttk.Button(
            button_frame,
            text="Add New Connection",
            command=self.add_new_connection
        )
        add_button.pack(side=tk.LEFT, fill='x', expand=True, padx=5)

        delete_button = ttk.Button(
            button_frame,
            text="Delete Selected",
            command=self.delete_connection
        )
        delete_button.pack(side=tk.RIGHT, fill='x', expand=True, padx=(5, 0))

    def create_about_ui(self):
        """Creates all widgets for the About tab."""
        title_label = ttk.Label(
            self.about_frame,
            text="Platform 1.0 Database Exporter",
            font=('Helvetica', 16, 'bold')
        )
        title_label.pack(pady=(10, 5))

        version_label = ttk.Label(
            self.about_frame,
            text="Version 1.2 (Structured Settings)",
            font=('Helvetica', 10, 'italic')
        )
        version_label.pack(pady=(0, 15))

        desc_label = ttk.Label(
            self.about_frame,
            text="This application allows Professional Services teams to export data from 1.0 Platform databases into Excel files for analysis and reporting purposes.",
            wraplength=350,  # Wrap text
            justify=tk.CENTER
        )
        desc_label.pack(pady=10)

        # --- MODIFICATION START ---
        # Create a frame to hold the "Developed by" text and link
        author_frame = ttk.Frame(self.about_frame)
        author_frame.pack(pady=10)

        dev_by_label = ttk.Label(
            author_frame,
            text="Developed by "
        )
        dev_by_label.pack(side=tk.LEFT)

        link_label = tk.Label(  # Use tk.Label for hyperlink effect
            author_frame,
            text="sriharan-certaintiai",
            fg="blue",
            cursor="hand2",
            font=('Helvetica', 10, 'underline')
        )
        link_label.pack(side=tk.LEFT)
        # Bind the click event to the open_link method
        link_label.bind("<Button-1>",
                        lambda e: self.open_link("https://github.com/sriharan-certaintiai/database_exporter"))

        company_label = ttk.Label(
            author_frame,
            text=". Certainti.Ai"
        )
        company_label.pack(side=tk.LEFT)
        # --- MODIFICATION END ---

    def open_link(self, url):
        """Opens the given URL in the default web browser."""
        try:
            webbrowser.open_new(url)
        except Exception as e:
            self.update_status(f"Could not open link: {e}", "error")
            messagebox.showerror("Error", f"Could not open browser for URL:\n{url}")

    def update_status(self, message, tag="info"):
        """Appends a new message to the status text widget."""
        self.status_text.config(state=tk.NORMAL)  # Enable writing
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.status_text.insert(tk.END, f"[{timestamp}] {message}\n", (tag,))
        self.status_text.see(tk.END)  # Auto-scroll to the bottom
        self.status_text.config(state=tk.DISABLED)  # Disable writing
        self.root.update_idletasks()  # Force UI update

    def load_config(self):
        """Loads connection info from config.ini into app state and comboboxes."""
        if not os.path.exists(CONFIG_FILE):
            self.update_status(f"Config file not found. Creating '{CONFIG_FILE}'...", "error")
            self.create_default_config()
            messagebox.showinfo("Config File Created",
                                f"A new config file '{CONFIG_FILE}' has been created.\n"
                                "Please go to the Settings tab to edit credentials.")
            # Still try to read the new file

        # Clear existing state
        self.connections = {}
        self.platform_to_section = {}
        self.config = configparser.ConfigParser()  # Re-init parser

        try:
            self.config.read(CONFIG_FILE)
            platform_names = []
            for section in self.config.sections():
                if not section.startswith('conn'):
                    continue

                # Store all details
                conn_details = dict(self.config[section])

                # Get the platform name (e.g., 'Platform A')
                platform_name = conn_details.get('name', section)
                platform_names.append(platform_name)

                # Map the user-friendly name back to its details and section
                self.connections[platform_name] = conn_details
                self.platform_to_section[platform_name] = section

            # Update both comboboxes
            self.platform_combo['values'] = platform_names
            self.settings_conn_combo['values'] = platform_names

            if platform_names:
                self.platform_combo.current(0)
                self.settings_conn_combo.current(0)
                self.update_status(f"Loaded {len(platform_names)} platform(s) from config.")
            else:
                self.update_status("Config file is empty. Please add a connection in Settings.", "error")

        except Exception as e:
            self.update_status(f"Error reading config file: {e}", "error")
            messagebox.showerror("Config Error", f"Could not parse '{CONFIG_FILE}'.\nError: {e}")

    def create_default_config(self):
        """Creates a default config.ini file."""
        default_config = configparser.ConfigParser()
        default_config['conn1'] = {
            'name': 'Platform A (Example)',
            'host': 'your_host_ip_or_name',
            'database': 'your_db_name',
            'user': 'your_username',
            'password': 'your_password',
            'query': ALLOWED_QUERY  # Use the constant
        }
        default_config['conn2'] = {
            'name': 'Platform B (Example)',
            'host': '127.0.0.1',
            'database': 'another_db',
            'user': 'script_user',
            'password': 'Script@2024$',
            'query': ALLOWED_QUERY  # Use the constant
        }

        try:
            with open(CONFIG_FILE, 'w') as configfile:
                default_config.write(configfile)
        except Exception as e:
            self.update_status(f"Failed to create default config: {e}", "error")

    def load_history(self):
        """Loads export history from history.json into the Treeview."""
        # Clear existing tree
        for item in self.history_tree.get_children():
            self.history_tree.delete(item)

        if not os.path.exists(HISTORY_FILE):
            self.history_data = []
            return

        try:
            with open(HISTORY_FILE, 'r') as f:
                self.history_data = json.load(f)

            # Insert data in reverse order (newest first)
            for record in reversed(self.history_data):
                self.history_tree.insert(
                    '',
                    tk.END,
                    values=(
                        record.get('platform', 'N/A'),
                        record.get('datetime', 'N/A'),
                        record.get('filename', 'N/A'),
                        record.get('filepath', 'N/A')
                    )
                )
        except Exception as e:
            self.update_status(f"Error loading history: {e}", "error")

    def add_to_history(self, platform_name, filepath):
        """Adds a new record to the history and saves it."""
        try:
            new_record = {
                'platform': platform_name,
                'datetime': datetime.now().isoformat(sep=' ', timespec='seconds'),
                'filename': os.path.basename(filepath),
                'filepath': os.path.abspath(filepath)
            }
            self.history_data.append(new_record)

            # Save to file
            with open(HISTORY_FILE, 'w') as f:
                json.dump(self.history_data, f, indent=4)

            # Update the Treeview
            self.history_tree.insert(
                '',
                0,  # Insert at the top
                values=(
                    new_record['platform'],
                    new_record['datetime'],
                    new_record['filename'],
                    new_record['filepath']
                )
            )
        except Exception as e:
            self.update_status(f"Failed to save history: {e}", "error")

    # --- New Settings Tab Methods ---

    def load_selected_conn_to_form(self, event=None):
        """Loads the selected connection's details into the settings form."""
        selected_name = self.settings_conn_combo.get()
        if not selected_name:
            # Clear form if nothing is selected
            self.settings_name_var.set("")
            self.settings_host_var.set("")
            self.settings_db_var.set("")
            self.settings_user_var.set("")
            self.settings_pass_var.set("")
            self.settings_query_var.set("")
            self.current_settings_section = None
            return

        try:
            section_name = self.platform_to_section[selected_name]
            details = self.connections[selected_name]

            self.current_settings_section = section_name  # Track current section

            # Populate form
            self.settings_name_var.set(details.get('name', ''))
            self.settings_host_var.set(details.get('host', ''))
            self.settings_db_var.set(details.get('database', ''))
            self.settings_user_var.set(details.get('user', ''))
            self.settings_pass_var.set(details.get('password', ''))
            self.settings_query_var.set(details.get('query', ''))

        except KeyError:
            self.update_status(f"Error loading '{selected_name}'. Try reloading config.", "error")

    def save_connection_changes(self):
        """Saves the current form data to the selected config section."""
        if not self.current_settings_section:
            messagebox.showwarning("No Connection", "No connection is selected to save.")
            return

        try:
            section = self.current_settings_section

            # Get data from form
            new_name = self.settings_name_var.get()
            if not new_name:
                messagebox.showerror("Validation Error", "Connection 'Name' cannot be empty.")
                return

            self.config.set(section, 'name', new_name)
            self.config.set(section, 'host', self.settings_host_var.get())
            self.config.set(section, 'database', self.settings_db_var.get())
            self.config.set(section, 'user', self.settings_user_var.get())
            self.config.set(section, 'password', self.settings_pass_var.get())
            # We explicitly DO NOT save the query, as it is read-only

            self.save_config_file_and_reload()

            # Reselect the item we just edited
            self.settings_conn_combo.set(new_name)
            self.load_selected_conn_to_form()

            messagebox.showinfo("Save Successful", f"Changes to '{new_name}' have been saved.")

        except Exception as e:
            self.update_status(f"Error saving changes: {e}", "error")
            messagebox.showerror("Save Error", f"Could not save changes.\nError: {e}")

    def add_new_connection(self):
        """Adds a new, default connection section to the config."""
        try:
            # Find a new unique section name (e.g., conn3, conn4)
            i = 1
            while f'conn{i}' in self.config.sections():
                i += 1
            new_section = f'conn{i}'
            new_name = f"New Connection {i}"

            # Add section and set defaults
            self.config.add_section(new_section)
            self.config.set(new_section, 'name', new_name)
            self.config.set(new_section, 'host', 'your_host')
            self.config.set(new_section, 'database', 'your_database')
            self.config.set(new_section, 'user', 'your_user')
            self.config.set(new_section, 'password', 'your_password')
            self.config.set(new_section, 'query', ALLOWED_QUERY)  # Requirement 2

            self.save_config_file_and_reload()

            # Select the new connection in the settings tab
            self.settings_conn_combo.set(new_name)
            self.load_selected_conn_to_form()
            self.update_status(f"Added new connection: '{new_name}'.", "success")

        except Exception as e:
            self.update_status(f"Error adding new connection: {e}", "error")
            messagebox.showerror("Error", f"Could not add new connection.\nError: {e}")

    def delete_connection(self):
        """Deletes the currently selected connection from the config."""
        if not self.current_settings_section:
            messagebox.showwarning("No Connection", "No connection is selected to delete.")
            return

        selected_name = self.settings_name_var.get()
        if not messagebox.askyesno("Confirm Delete",
                                   f"Are you sure you want to delete '{selected_name}'?\n"
                                   "This action cannot be undone."):
            return

        try:
            self.config.remove_section(self.current_settings_section)
            self.save_config_file_and_reload()

            # Form will be reloaded with the first item
            self.update_status(f"Deleted connection: '{selected_name}'.", "success")

        except Exception as e:
            self.update_status(f"Error deleting connection: {e}", "error")
            messagebox.showerror("Error", f"Could not delete connection.\nError: {e}")

    def save_config_file_and_reload(self):
        """Helper function to write config to file and reload app state."""
        try:
            with open(CONFIG_FILE, 'w') as configfile:
                self.config.write(configfile)
        except Exception as e:
            self.update_status(f"Failed to write config file: {e}", "error")
            messagebox.showerror("Save Error", f"Could not write to {CONFIG_FILE}.\nError: {e}")
            return

        self.update_status("Config file saved. Reloading platforms...", "info")
        self.load_config()  # This reloads all internal state and comboboxes

    # --- End New Settings Tab Methods ---

    def start_export_thread(self):
        """
        Starts the database export process in a separate thread
        to avoid freezing the GUI.
        """
        selected_platform = self.platform_combo.get()
        if not selected_platform:
            messagebox.showwarning("No Platform", "Please select a platform first.")
            return

        # Get connection details for the selected platform
        if selected_platform not in self.connections:
            messagebox.showerror("Error", f"Could not find connection details for '{selected_platform}'.")
            return

        conn_details = self.connections[selected_platform]

        # --- Start UI feedback ---
        self.export_button.config(state=tk.DISABLED, text="Exporting...")
        self.progress_bar.start()
        self.status_text.config(state=tk.NORMAL)  # Clear previous log
        self.status_text.delete('1.0', tk.END)
        self.status_text.config(state=tk.DISABLED)
        self.update_status(f"Starting export for '{selected_platform}'...")

        # --- Start worker thread ---
        # Pass the queue, connection details, and platform name
        threading.Thread(
            target=self.run_export_logic,
            args=(conn_details, selected_platform),
            daemon=True
        ).start()

    def run_export_logic(self, conn_details, platform_name):
        """
        This function runs in a separate thread.
        It connects to the DB, fetches data, and puts the
        result (DataFrame or Error) into the queue.
        """
        try:
            # --- 1. Get connection details from dict ---
            host = conn_details.get('host')
            database = conn_details.get('database')
            user = conn_details.get('user')
            password = conn_details.get('password')
            sql_query = conn_details.get('query')

            if not all([host, database, user, password, sql_query]):
                raise ValueError("Missing connection details in config file (host, database, user, password, query).")

            # --- REQUIREMENT 3: Check if query is allowed ---
            # Normalize both queries for a robust comparison
            # 1. Strip whitespace, 2. Replace multiple spaces with one, 3. Remove trailing semicolon, 4. To lowercase
            normalized_query = ' '.join(sql_query.strip().split()).rstrip(';').lower()
            normalized_allowed = ' '.join(ALLOWED_QUERY.strip().split()).rstrip(';').lower()

            if normalized_query != normalized_allowed:
                # Raise an error that will be caught and sent to the queue
                raise ValueError(f"Query Not Allowed: Only '{ALLOWED_QUERY}' is permitted by policy.")
            # --- End Query Check ---

            self.export_queue.put(("status", "Connecting to database..."))

            # --- 2. Database Connection ---
            connection = mysql.connector.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )

            if connection.is_connected():
                self.export_queue.put(("status", "Successfully connected. Executing query..."))

                # --- 3. Read data into DataFrame ---
                df = pd.read_sql(sql_query, connection)
                self.export_queue.put(("status", f"Successfully fetched {len(df)} rows."))

                # --- 4. Clean data (from original script) ---
                self.export_queue.put(("status", "Cleaning data for Excel compatibility..."))
                illegal_xml_chars_re = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')

                def clean_string(value):
                    if isinstance(value, str):
                        return illegal_xml_chars_re.sub('', value)
                    return value

                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].apply(clean_string)

                self.export_queue.put(("status", "Cleaning complete. Data is ready."))

                # --- 5. Put successful result in queue ---
                # We send the dataframe and platform name for the save dialog
                self.export_queue.put(("success", (df, platform_name)))

        except Error as e:
            # Handle DB errors
            self.export_queue.put(("error", f"Database Error: {e}"))
        except Exception as e:
            # Handle other errors (config, pandas, value errors)
            self.export_queue.put(("error", f"An Error Occurred: {e}"))

        finally:
            # --- 6. Close connection ---
            if 'connection' in locals() and connection.is_connected():
                connection.close()
                self.export_queue.put(("status", "Database connection closed."))

    def check_queue(self):
        """
        Checks the queue for messages from the worker thread
        and updates the GUI accordingly.
        """
        try:
            # Check for messages without blocking
            while True:
                msg_type, data = self.export_queue.get_nowait()

                if msg_type == "status":
                    self.update_status(data, "info")

                elif msg_type == "error":
                    self.update_status(data, "error")
                    self.stop_export_feedback()
                    messagebox.showerror("Export Failed", data)

                elif msg_type == "success":
                    self.update_status("Data fetched! Please choose where to save the file.", "success")
                    self.stop_export_feedback()

                    # Unpack data
                    df, platform_name = data

                    # --- Ask user for save location ---
                    self.prompt_save_file(df, platform_name)

        except queue.Empty:
            # No messages in queue, just check again later
            pass
        finally:
            # Schedule the next check
            self.root.after(100, self.check_queue)

    def stop_export_feedback(self):
        """Resets the export button and progress bar."""
        self.progress_bar.stop()
        self.export_button.config(state=tk.NORMAL, text="Start Export...")

    def prompt_save_file(self, df, platform_name):
        """
        Prompts the user to select a save location and
        saves the DataFrame to an Excel file.
        """
        # Suggest a filename
        safe_name = re.sub(r'[^a-z0-9_]', '', platform_name.lower().replace(' ', '_'))
        default_filename = f"{safe_name}_export_{datetime.now().strftime('%Y%m%d')}.xlsx"

        filepath = filedialog.asksaveasfilename(
            title="Save Excel File",
            initialfile=default_filename,
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )

        if not filepath:
            self.update_status("Save operation cancelled by user.", "info")
            return

        # --- Save the file (this is fast, so no new thread needed) ---
        try:
            self.update_status(f"Saving file to {filepath}...", "info")
            df.to_excel(filepath, index=False, engine='openpyxl')
            self.update_status(f"Export complete! File saved successfully.", "success")

            # --- Add to history ---
            self.add_to_history(platform_name, filepath)

            messagebox.showinfo("Export Complete", f"File saved successfully to:\n{filepath}")

        except Exception as e:
            self.update_status(f"Failed to save file: {e}", "error")
            messagebox.showerror("Save Error", f"Could not save the file.\nError: {e}")


# --- Main entry point ---
if __name__ == "__main__":
    try:
        root = tk.Tk()
        app = DbExporterApp(root)
        root.mainloop()
    except Exception as e:
        # A final catch-all for any startup errors
        print(f"Failed to start application: {e}")
        # Use a simple tk messagebox if the main app failed to init
        if 'root' not in locals():
            root = tk.Tk()
            root.withdraw()  # Hide the main window
        messagebox.showerror("Application Error", f"A critical error occurred:\n{e}")