#!/usr/bin/env python3
"""
Manual Editor Tool
Basic GUI for manually editing departments, locations, and their synonyms.
"""

import os
import sqlite3
import sys
import tkinter as tk
from tkinter import messagebox, ttk

# Add parent directory to path to import db_utils
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app.services.db_utils import JOBS_DB, JobsDB, get_db_connection


class ManualEditor:
    def __init__(self, root):
        self.root = root
        self.root.title("Manual Editor - Departments & Locations")
        self.root.geometry("900x700")

        self.jobs_db = JobsDB()
        # Create notebook for tabs
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Create tabs
        self.departments_frame = ttk.Frame(self.notebook)
        self.locations_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.departments_frame, text="Departments")
        self.notebook.add(self.locations_frame, text="Locations")

        # Initialize tabs
        self.setup_departments_tab()
        self.setup_locations_tab()

        # Load initial data
        self.refresh_departments()
        self.refresh_locations()

    def setup_departments_tab(self):
        """Setup the departments editing tab"""
        # Left panel: list of departments
        left_frame = ttk.Frame(self.departments_frame)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)

        ttk.Label(left_frame, text="Departments:", font=("Arial", 12, "bold")).pack(anchor=tk.W)

        # Listbox with scrollbar
        list_frame = ttk.Frame(left_frame)
        list_frame.pack(fill=tk.BOTH, expand=True)

        self.dept_Listbox = tk.Listbox(list_frame, height=20)
        self.dept_scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.dept_Listbox.yview)
        self.dept_Listbox.config(yscrollcommand=self.dept_scrollbar.set)
        self.dept_Listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.dept_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.dept_Listbox.bind("<<ListboxSelect>>", self.on_department_select)

        # Right panel: Edit form
        right_frame = ttk.Frame(self.departments_frame)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Button frame for New/Save
        button_frame = ttk.Frame(right_frame)
        button_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(right_frame, text="Edit Department:", font=("Arial", 12, "bold")).pack(anchor=tk.W, pady=(0, 10))

        ttk.Button(button_frame, text="New Department", command=self.new_department).pack(side=tk.LEFT, padx=(0, 5))

        # Department ID (read-only)
        ttk.Label(right_frame, text="ID:").pack(anchor=tk.W)
        self.dept_id_var = tk.StringVar()
        ttk.Entry(right_frame, textvariable=self.dept_id_var, state="readonly", width=50).pack(fill=tk.X, pady=(0, 10))

        # Canonical Name
        ttk.Label(right_frame, text="Canonical Name:").pack(anchor=tk.W)
        self.dept_canonical_var = tk.StringVar()
        ttk.Entry(right_frame, textvariable=self.dept_canonical_var, width=50).pack(fill=tk.X, pady=(0, 10))

        # Category
        ttk.Label(right_frame, text="Category:").pack(anchor=tk.W)
        self.dept_category_var = tk.StringVar()
        ttk.Entry(right_frame, textvariable=self.dept_category_var, width=50).pack(fill=tk.X, pady=(0, 10))

        # Save/Delete buttons
        save_delete_frame = ttk.Frame(right_frame)
        save_delete_frame.pack(fill=tk.X, pady=10)
        ttk.Button(save_delete_frame, text="Save Changes", command=self.save_department).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(save_delete_frame, text="Delete Department", command=self.delete_department).pack(side=tk.LEFT)

        # Synonyms section
        ttk.Separator(right_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=20)
        ttk.Label(right_frame, text="Synonyms:", font=("Arial", 10, "bold")).pack(anchor=tk.W)

        # Synonyms Listbox
        synonyms_frame = ttk.Frame(right_frame)
        synonyms_frame.pack(fill=tk.BOTH, expand=True)

        self.dept_synonyms_Listbox = tk.Listbox(synonyms_frame, height=8)
        self.dept_synonyms_scrollbar = ttk.Scrollbar(
            synonyms_frame, orient=tk.VERTICAL, command=self.dept_synonyms_Listbox.yview
        )
        self.dept_synonyms_Listbox.config(yscrollcommand=self.dept_synonyms_scrollbar.set)
        self.dept_synonyms_Listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.dept_synonyms_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Add/Remove synonym buttons
        synonym_buttons_frame = ttk.Frame(right_frame)
        synonym_buttons_frame.pack(fill=tk.X, pady=5)

        ttk.Label(synonym_buttons_frame, text="New Synonym:").pack(side=tk.LEFT, padx=(0, 5))
        self.dept_new_synonym_var = tk.StringVar()
        ttk.Entry(synonym_buttons_frame, textvariable=self.dept_new_synonym_var, width=30).pack(
            side=tk.LEFT, padx=(0, 5)
        )
        ttk.Button(synonym_buttons_frame, text="Add", command=self.add_department_synonym).pack(
            side=tk.LEFT, padx=(0, 5)
        )
        ttk.Button(synonym_buttons_frame, text="Remove Selected", command=self.remove_department_synonym).pack(
            side=tk.LEFT
        )

        # Refresh button
        ttk.Button(left_frame, text="Refresh list", command=self.refresh_departments).pack(pady=5)

        self.current_dept_id = None

    def setup_locations_tab(self):
        """Setup the locations editing tab"""
        # Left panel: list of locations
        left_frame = ttk.Frame(self.locations_frame)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)

        ttk.Label(left_frame, text="Locations:", font=("Arial", 12, "bold")).pack(anchor=tk.W)

        # Listbox with scrollbar
        list_frame = ttk.Frame(left_frame)
        list_frame.pack(fill=tk.BOTH, expand=True)

        self.loc_Listbox = tk.Listbox(list_frame, height=20)
        self.loc_scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.loc_Listbox.yview)
        self.loc_Listbox.config(yscrollcommand=self.loc_scrollbar.set)
        self.loc_Listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.loc_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.loc_Listbox.bind("<<ListboxSelect>>", self.on_location_select)

        # Right panel: Edit form
        right_frame = ttk.Frame(self.locations_frame)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Button frame for New/Save
        button_frame = ttk.Frame(right_frame)
        button_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(right_frame, text="Edit Location:", font=("Arial", 12, "bold")).pack(anchor=tk.W, pady=(0, 10))

        ttk.Button(button_frame, text="New Location", command=self.new_location).pack(side=tk.LEFT, padx=(0, 5))

        # Location ID (read-only)
        ttk.Label(right_frame, text="ID:").pack(anchor=tk.W)
        self.loc_id_var = tk.StringVar()
        ttk.Entry(right_frame, textvariable=self.loc_id_var, state="readonly", width=50).pack(fill=tk.X, pady=(0, 10))

        # Canonical Name
        ttk.Label(right_frame, text="Canonical Name:").pack(anchor=tk.W)
        self.loc_canonical_var = tk.StringVar()
        ttk.Entry(right_frame, textvariable=self.loc_canonical_var, width=50).pack(fill=tk.X, pady=(0, 10))

        # Country
        ttk.Label(right_frame, text="Country:").pack(anchor=tk.W)
        self.loc_country_var = tk.StringVar()
        ttk.Entry(right_frame, textvariable=self.loc_country_var, width=50).pack(fill=tk.X, pady=(0, 10))

        # Region
        ttk.Label(right_frame, text="Region:").pack(anchor=tk.W)
        self.loc_region_var = tk.StringVar()
        ttk.Entry(right_frame, textvariable=self.loc_region_var, width=50).pack(fill=tk.X, pady=(0, 10))

        # Save/Delete buttons
        save_delete_frame = ttk.Frame(right_frame)
        save_delete_frame.pack(fill=tk.X, pady=10)
        ttk.Button(save_delete_frame, text="Save Changes", command=self.save_location).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(save_delete_frame, text="Delete Location", command=self.delete_location).pack(side=tk.LEFT)

        # Synonyms section
        ttk.Separator(right_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=20)
        ttk.Label(right_frame, text="Synonyms:", font=("Arial", 10, "bold")).pack(anchor=tk.W)

        # Synonyms Listbox
        synonyms_frame = ttk.Frame(right_frame)
        synonyms_frame.pack(fill=tk.BOTH, expand=True)

        self.loc_synonyms_Listbox = tk.Listbox(synonyms_frame, height=8)
        self.loc_synonyms_scrollbar = ttk.Scrollbar(
            synonyms_frame, orient=tk.VERTICAL, command=self.loc_synonyms_Listbox.yview
        )
        self.loc_synonyms_Listbox.config(yscrollcommand=self.loc_synonyms_scrollbar.set)
        self.loc_synonyms_Listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.loc_synonyms_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Add/Remove synonym buttons
        synonym_buttons_frame = ttk.Frame(right_frame)
        synonym_buttons_frame.pack(fill=tk.X, pady=5)

        ttk.Label(synonym_buttons_frame, text="New Synonym:").pack(side=tk.LEFT, padx=(0, 5))
        self.loc_new_synonym_var = tk.StringVar()
        ttk.Entry(synonym_buttons_frame, textvariable=self.loc_new_synonym_var, width=30).pack(
            side=tk.LEFT, padx=(0, 5)
        )
        ttk.Button(synonym_buttons_frame, text="Add", command=self.add_location_synonym).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(synonym_buttons_frame, text="Remove Selected", command=self.remove_location_synonym).pack(
            side=tk.LEFT
        )

        # Refresh button
        ttk.Button(left_frame, text="Refresh list", command=self.refresh_locations).pack(pady=5)

        self.current_loc_id = None

    def refresh_departments(self):
        """Refresh the departments list"""
        departments = self.jobs_db.get_all_departments_sync()
        self.dept_Listbox.delete(0, tk.END)
        self.departments_data = {}

        for dept in departments:
            display_name = f"{dept['canonical_name']} ({dept['category'] or 'No category'})"
            self.dept_Listbox.insert(tk.END, display_name)
            self.departments_data[dept["id"]] = dept

    def refresh_locations(self):
        """Refresh the locations list"""
        locations = self.jobs_db.get_all_locations()
        self.loc_Listbox.delete(0, tk.END)
        self.locations_data = {}

        for loc in locations:
            country_region = f"{loc['country'] or ''}, {loc['region'] or ''}".strip(", ")
            if country_region:
                display_name = f"{loc['canonical_name']} ({country_region})"
            else:
                display_name = loc["canonical_name"]
            self.loc_Listbox.insert(tk.END, display_name)
            self.locations_data[loc["id"]] = loc

    def on_department_select(self, event):
        """Handle department selection"""
        selection = self.dept_Listbox.curselection()
        if not selection:
            return

        index = selection[0]
        dept_id = list(self.departments_data.keys())[index]
        dept = self.departments_data[dept_id]

        self.current_dept_id = dept_id
        self.dept_id_var.set(str(dept_id))
        self.dept_canonical_var.set(dept["canonical_name"] or "")
        self.dept_category_var.set(dept["category"] or "")

        # Load synonyms
        self.load_department_synonyms(dept_id)

    def on_location_select(self, event):
        """Handle location selection"""
        selection = self.loc_Listbox.curselection()
        if not selection:
            return

        index = selection[0]
        loc_id = list(self.locations_data.keys())[index]
        loc = self.locations_data[loc_id]

        self.current_loc_id = loc_id
        self.loc_id_var.set(str(loc_id))
        self.loc_canonical_var.set(loc["canonical_name"] or "")
        self.loc_country_var.set(loc["country"] or "")
        self.loc_region_var.set(loc["region"] or "")

        # Load synonyms
        self.load_location_synonyms(loc_id)

    def load_department_synonyms(self, dept_id: int):
        """Load synonyms for a department"""
        self.dept_synonyms_Listbox.delete(0, tk.END)

        with get_db_connection(JOBS_DB) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT synonym FROM department_synonyms WHERE department_id = ? ORDER BY synonym", (dept_id,)
            )
            for row in cursor.fetchall():
                self.dept_synonyms_Listbox.insert(tk.END, row[0])

    def load_location_synonyms(self, loc_id: int):
        """Load synonyms for a location"""
        self.loc_synonyms_Listbox.delete(0, tk.END)

        with get_db_connection(JOBS_DB) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT synonym FROM location_synonyms WHERE location_id = ? ORDER BY synonym", (loc_id,))
            for row in cursor.fetchall():
                self.loc_synonyms_Listbox.insert(tk.END, row[0])

    def new_department(self):
        """Clear form for new department"""
        self.current_dept_id = None
        self.dept_id_var.set("(New)")
        self.dept_canonical_var.set("")
        self.dept_category_var.set("")
        self.dept_synonyms_Listbox.delete(0, tk.END)
        self.dept_Listbox.selection_clear(0, tk.END)

    def new_location(self):
        """Clear form for new location"""
        self.current_loc_id = None
        self.loc_id_var.set("(New)")
        self.loc_canonical_var.set("")
        self.loc_country_var.set("")
        self.loc_region_var.set("")
        self.loc_synonyms_Listbox.delete(0, tk.END)
        self.loc_Listbox.selection_clear(0, tk.END)

    def save_department(self):
        """Save department changes or create new department"""
        canonical_name = self.dept_canonical_var.get().strip()
        if not canonical_name:
            messagebox.showerror("Error", "Canonical name cannot be empty.")
            return

        category = self.dept_category_var.get().strip() or None

        try:
            with get_db_connection(JOBS_DB) as conn:
                cursor = conn.cursor()

                if self.current_dept_id is None:
                    # Creating new department
                    cursor.execute(
                        "INSERT INTO departments (canonical_name, category) VALUES (?, ?)", (canonical_name, category)
                    )
                    new_id = cursor.lastrowid
                    conn.commit()
                    messagebox.showinfo("Success", f"Department created successfully with ID {new_id}.")
                    self.current_dept_id = new_id
                    self.dept_id_var.set(str(new_id))
                else:
                    # Updating existing department
                    cursor.execute(
                        "UPDATE departments SET canonical_name = ?, category = ? WHERE id = ?",
                        (canonical_name, category, self.current_dept_id),
                    )
                    conn.commit()
                    messagebox.showinfo("Success", "Department updated successfully.")

            self.refresh_departments()
            # Select the current department in the list
            if self.current_dept_id:
                dept_ids = list(self.departments_data.keys())
                if self.current_dept_id in dept_ids:
                    index = dept_ids.index(self.current_dept_id)
                    self.dept_Listbox.selection_clear(0, tk.END)
                    self.dept_Listbox.selection_set(index)
                    self.dept_Listbox.see(index)
        except sqlite3.IntegrityError:
            messagebox.showerror("Error", "A department with this canonical name already exists.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save department: {e}")

    def save_location(self):
        """Save location changes or create new location"""
        canonical_name = self.loc_canonical_var.get().strip()
        if not canonical_name:
            messagebox.showerror("Error", "Canonical name cannot be empty.")
            return

        country = self.loc_country_var.get().strip() or None
        region = self.loc_region_var.get().strip() or None

        try:
            with get_db_connection(JOBS_DB) as conn:
                cursor = conn.cursor()

                if self.current_loc_id is None:
                    # Creating new location
                    cursor.execute(
                        "INSERT INTO locations (canonical_name, country, region) VALUES (?, ?, ?)",
                        (canonical_name, country, region),
                    )
                    new_id = cursor.lastrowid
                    conn.commit()
                    messagebox.showinfo("Success", f"Location created successfully with ID {new_id}.")
                    self.current_loc_id = new_id
                    self.loc_id_var.set(str(new_id))
                else:
                    # Updating existing location
                    cursor.execute(
                        "UPDATE locations SET canonical_name = ?, country = ?, region = ? WHERE id = ?",
                        (canonical_name, country, region, self.current_loc_id),
                    )
                    conn.commit()
                    messagebox.showinfo("Success", "Location updated successfully.")

            self.refresh_locations()
            # Select the current location in the list
            if self.current_loc_id:
                loc_ids = list(self.locations_data.keys())
                if self.current_loc_id in loc_ids:
                    index = loc_ids.index(self.current_loc_id)
                    self.loc_Listbox.selection_clear(0, tk.END)
                    self.loc_Listbox.selection_set(index)
                    self.loc_Listbox.see(index)
        except sqlite3.IntegrityError:
            messagebox.showerror("Error", "A location with this canonical name already exists.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save location: {e}")

    def add_department_synonym(self):
        """Add a new department synonym"""
        if self.current_dept_id is None:
            messagebox.showwarning("Warning", "Please create or select a department first.")
            return

        synonym = self.dept_new_synonym_var.get().strip()
        if not synonym:
            messagebox.showerror("Error", "Synonym cannot be empty.")
            return

        try:
            with get_db_connection(JOBS_DB) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO department_synonyms (synonym, department_id) VALUES (?, ?)",
                    (synonym, self.current_dept_id),
                )
                conn.commit()

            self.dept_new_synonym_var.set("")
            self.load_department_synonyms(self.current_dept_id)
            messagebox.showinfo("Success", "Synonym added successfully.")
        except sqlite3.IntegrityError:
            messagebox.showerror("Error", "This synonym already exists.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to add synonym: {e}")

    def remove_department_synonym(self):
        """Remove selected department synonym"""
        if self.current_dept_id is None:
            messagebox.showwarning("Warning", "Please create or select a department first.")
            return

        selection = self.dept_synonyms_Listbox.curselection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a synonym to remove.")
            return

        synonym = self.dept_synonyms_Listbox.get(selection[0])

        if not messagebox.askyesno("Confirm", f"Remove synonym '{synonym}'?"):
            return

        try:
            with get_db_connection(JOBS_DB) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM department_synonyms WHERE synonym = ? AND department_id = ?",
                    (synonym, self.current_dept_id),
                )
                conn.commit()

            self.load_department_synonyms(self.current_dept_id)
            messagebox.showinfo("Success", "Synonym removed successfully.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to remove synonym: {e}")

    def add_location_synonym(self):
        """Add a new location synonym"""
        if self.current_loc_id is None:
            messagebox.showwarning("Warning", "Please create or select a location first.")
            return

        synonym = self.loc_new_synonym_var.get().strip()
        if not synonym:
            messagebox.showerror("Error", "Synonym cannot be empty.")
            return

        try:
            with get_db_connection(JOBS_DB) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO location_synonyms (synonym, location_id) VALUES (?, ?)", (synonym, self.current_loc_id)
                )
                conn.commit()

            self.loc_new_synonym_var.set("")
            self.load_location_synonyms(self.current_loc_id)
            messagebox.showinfo("Success", "Synonym added successfully.")
        except sqlite3.IntegrityError:
            messagebox.showerror("Error", "This synonym already exists.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to add synonym: {e}")

    def remove_location_synonym(self):
        """Remove selected location synonym"""
        if self.current_loc_id is None:
            messagebox.showwarning("Warning", "Please create or select a location first.")
            return

        selection = self.loc_synonyms_Listbox.curselection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a synonym to remove.")
            return

        synonym = self.loc_synonyms_Listbox.get(selection[0])

        if not messagebox.askyesno("Confirm", f"Remove synonym '{synonym}'?"):
            return

        try:
            with get_db_connection(JOBS_DB) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM location_synonyms WHERE synonym = ? AND location_id = ?",
                    (synonym, self.current_loc_id),
                )
                conn.commit()

            self.load_location_synonyms(self.current_loc_id)
            messagebox.showinfo("Success", "Synonym removed successfully.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to remove synonym: {e}")

    def delete_department(self):
        """Delete a department"""
        if self.current_dept_id is None:
            messagebox.showwarning("Warning", "Please select a department to delete.")
            return

        canonical_name = self.dept_canonical_var.get().strip()

        # Check how many jobs reference this department
        try:
            with get_db_connection(JOBS_DB) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM jobs WHERE department_id = ?", (self.current_dept_id,))
                job_count = cursor.fetchone()[0]
        except Exception as e:
            messagebox.showerror("Error", f"Failed to check department usage: {e}")
            return

        # Build warning message
        warning_msg = f"Are you sure you want to delete department '{canonical_name}'?\n\n"
        if job_count > 0:
            warning_msg += f"WARNING: {job_count} job(s) reference this department.\n"
            warning_msg += "Their department_id will be set to NULL.\n\n"
        warning_msg += "All synonyms for this department will also be deleted.\n\n"
        warning_msg += "This action cannot be undone!"

        if not messagebox.askyesno("Confirm Delete", warning_msg):
            return

        try:
            with get_db_connection(JOBS_DB) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM departments WHERE id = ?", (self.current_dept_id,))
                conn.commit()

            messagebox.showinfo("Success", f"Department '{canonical_name}' deleted successfully.")

            # Clear the form
            self.current_dept_id = None
            self.dept_id_var.set("")
            self.dept_canonical_var.set("")
            self.dept_category_var.set("")
            self.dept_synonyms_Listbox.delete(0, tk.END)
            self.dept_Listbox.selection_clear(0, tk.END)

            # Refresh the list
            self.refresh_departments()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to delete department: {e}")

    def delete_location(self):
        """Delete a location"""
        if self.current_loc_id is None:
            messagebox.showwarning("Warning", "Please select a location to delete.")
            return

        canonical_name = self.loc_canonical_var.get().strip()

        # Check how many jobs reference this location
        try:
            with get_db_connection(JOBS_DB) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM jobs WHERE location_id = ?", (self.current_loc_id,))
                job_count = cursor.fetchone()[0]
        except Exception as e:
            messagebox.showerror("Error", f"Failed to check location usage: {e}")
            return

        # Build warning message
        warning_msg = f"Are you sure you want to delete location '{canonical_name}'?\n\n"
        if job_count > 0:
            warning_msg += f"WARNING: {job_count} job(s) reference this location.\n"
            warning_msg += "Their location_id will be set to NULL.\n\n"
        warning_msg += "All synonyms for this location will also be deleted.\n\n"
        warning_msg += "This action cannot be undone!"

        if not messagebox.askyesno("Confirm Delete", warning_msg):
            return

        try:
            with get_db_connection(JOBS_DB) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM locations WHERE id = ?", (self.current_loc_id,))
                conn.commit()

            messagebox.showinfo("Success", f"Location '{canonical_name}' deleted successfully.")

            # Clear the form
            self.current_loc_id = None
            self.loc_id_var.set("")
            self.loc_canonical_var.set("")
            self.loc_country_var.set("")
            self.loc_region_var.set("")
            self.loc_synonyms_Listbox.delete(0, tk.END)
            self.loc_Listbox.selection_clear(0, tk.END)

            # Refresh the list
            self.refresh_locations()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to delete location: {e}")


def main():
    root = tk.Tk()
    app = ManualEditor(root)
    root.mainloop()


if __name__ == "__main__":
    main()
