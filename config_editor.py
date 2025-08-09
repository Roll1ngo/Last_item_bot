import tkinter as tk
from tkinter import messagebox
import yaml
import re
from collections import OrderedDict
import os

# Зареєструємо репрезентатор, щоб PyYAML зберігав OrderedDict як звичайний словник
# Це запобіжить появі тегів !!python/object/apply:collections.OrderedDict
yaml.add_representer(OrderedDict, lambda dumper, data: dumper.represent_dict(data.items()))


# ---------- Парсер структури + коментарів (щоб зберегти порядок з файлу) ----------
def parse_yaml_structure_and_comments(path):
    """
    Returns:
      - template: An OrderedDict containing the same nested key structure, in the file's order.
      - comments: A dict mapping tuple(path) -> comment_text (the string after '#')
    We read the raw lines to preserve key order and capture comments.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except FileNotFoundError:
        messagebox.showerror("Error", f"Configuration file '{path}' not found.")
        return OrderedDict(), {}

    root = OrderedDict()
    # Stack of elements: (indent, key, dict_ref)
    stack = [(-1, None, root)]
    comments = {}

    key_path_stack = []  # Tracks the key path to easily build tuple(path)

    for raw in lines:
        line = raw.rstrip("\n")
        if not line.strip() or line.lstrip().startswith("#"):
            continue

        m = re.match(r'^(\s*)([^\s:#]+)\s*:', line)
        if not m:
            continue

        indent = len(m.group(1))
        key = m.group(2)

        # Find the parent based on indentation
        while stack and stack[-1][0] >= indent:
            stack.pop()
            if key_path_stack:
                key_path_stack.pop()

        if not stack:
            # This can happen if the YAML file has incorrect indentation
            continue

        parent_dict = stack[-1][2]

        # Add the key to the parent's OrderedDict (placeholder is an empty OrderedDict)
        # Then, when merging values, we will replace the empty ones with real scalars or dictionaries
        parent_dict[key] = OrderedDict()

        # Build the full path as a tuple
        path_tuple = tuple(key_path_stack + [key])

        # Extract the comment (everything after '#'), if any
        if "#" in line:
            comment = line.split("#", 1)[1].strip()
            comments[path_tuple] = comment

        # Push the new element onto the stack as a potential parent for subsequent lines
        stack.append((indent, key, parent_dict[key]))
        key_path_stack.append(key)

    return root, comments


def build_ordered_with_values(template, values):
    """
    Returns an OrderedDict built from the template (which preserves order),
    but with values from `values` (returned by yaml.safe_load).
    If a key in `values` has a dict -> recursively build.
    Otherwise, substitute a scalar/list.
    """
    result = OrderedDict()
    for k, v in template.items():
        val = values.get(k) if isinstance(values, dict) else None
        if isinstance(val, dict):
            # If the value is a dictionary, recurse
            result[k] = build_ordered_with_values(v, val)
        else:
            # If the value is a scalar or doesn't exist, substitute what's available
            # (if not in values, do nothing -> None)
            result[k] = val
    return result


# ---------- GUI Editor ----------
class ConfigEditor(tk.Tk):
    def __init__(self, config_file_path):
        super().__init__()
        self.title("YAML Config Editor")
        self.geometry("900x700")
        self.config_file_path = config_file_path

        if not os.path.exists(config_file_path):
            messagebox.showerror("Error", f"Configuration file '{config_file_path}' not found.")
            self.destroy()
            return

        if not self._load_and_build_config():
            # Якщо завантаження не вдалося, знищуємо вікно і виходимо з конструктора
            self.destroy()
            return

        self._setup_ui()

    def _load_and_build_config(self):
        """Loads and builds the configuration from the file. Returns False on failure."""
        template, comments = parse_yaml_structure_and_comments(self.config_file_path)
        self.comments = comments  # Keys as tuple paths

        with open(self.config_file_path, "r", encoding="utf-8") as f:
            try:
                loaded_values = yaml.safe_load(f) or {}
            except yaml.YAMLError as e:
                messagebox.showerror("Error", f"Error reading YAML: {e}")
                return False

        self.ordered_config = build_ordered_with_values(template, loaded_values)
        self.original_values = loaded_values
        return True

    def _setup_ui(self):
        """Sets up the user interface."""
        main_frame = tk.Frame(self)
        main_frame.pack(fill=tk.BOTH, expand=True)

        self.canvas = tk.Canvas(main_frame)
        scrollbar = tk.Scrollbar(main_frame, orient="vertical", command=self.canvas.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.scrollable_frame = tk.Frame(self.canvas)
        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.scrollable_frame.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)
        self.canvas.bind_all("<Button-4>", self._on_mousewheel)
        self.canvas.bind_all("<Button-5>", self._on_mousewheel)

        self.widgets = {}
        self.current_row = 0

        for widget in self.scrollable_frame.winfo_children():
            widget.destroy()

        self.create_entry_widgets(self.scrollable_frame, self.ordered_config, prefix="")

        btn_frame = tk.Frame(self)
        btn_frame.pack(fill=tk.X, pady=6)
        tk.Button(btn_frame, text="Save", command=self.save_config).pack(side=tk.RIGHT, padx=10)

    def _on_mousewheel(self, event):
        """Mouse wheel event handler for scrolling."""
        if event.delta:
            self.canvas.yview_scroll(-int(event.delta / 120), "units")
        else:
            if event.num == 4:
                self.canvas.yview_scroll(-1, "units")
            elif event.num == 5:
                self.canvas.yview_scroll(1, "units")

    def get_comment_for_path(self, prefix, key):
        """Returns the comment for a given key path."""
        if prefix:
            parts = tuple(prefix.split(".")) + (key,)
        else:
            parts = (key,)
        return self.comments.get(parts, "")

    def create_entry_widgets(self, parent, data, prefix="", indent_level=0):
        """
        Recursively creates widgets based on ordered data with indentation.
        prefix - e.g., "top_minimal" or "log_colors"
        indent_level - the current indentation level for visual grouping
        """
        for key, val in data.items():
            full_key = f"{prefix}.{key}" if prefix else key

            # Determine the background color for alternating rows
            # Змінено світлий сірий на #f5f5f5 та темний на #d0d0d0
            bg_color = "#f5f5f5" if self.current_row % 2 == 0 else "#d0d0d0"

            # Determine indentation for the current level
            padx_label = (12 + indent_level * 20, 0)

            # If it's the special "char" + "value" structure, combine the label and create an entry field
            if isinstance(val, OrderedDict) and 'char' in val and 'value' in val:
                combined_label_text = f"{key}: {val['char']}"

                row_frame = tk.Frame(parent, bg=bg_color)
                row_frame.grid(row=self.current_row, column=0, columnspan=2, sticky="ew")

                lbl = tk.Label(row_frame, text=combined_label_text, anchor="w", justify="left", bg=bg_color)
                lbl.pack(side="left", padx=padx_label, pady=2, expand=True, fill="x")

                entry = tk.Entry(row_frame, width=50)
                entry.insert(0, str(val['value']))
                entry.pack(side="right", padx=6)
                self.widgets[f"{full_key}.value"] = entry

                self.current_row += 1
            # If it's a regular dictionary, create a header and make a recursive call
            elif isinstance(val, OrderedDict):
                orig_val = self._get_original_value(full_key)
                if isinstance(orig_val, dict):
                    # Add a visual separator before a new block
                    if self.current_row > 0:
                        separator = tk.Frame(parent, height=1, bg="gray")
                        separator.grid(row=self.current_row, column=0, columnspan=2, sticky="ew",
                                       padx=(10 + indent_level * 20, 10), pady=(10, 0))
                        self.current_row += 1

                    header_frame = tk.Frame(parent, bg="#f5f5f5")
                    header_frame.grid(row=self.current_row, column=0, columnspan=2, sticky="ew")

                    header = tk.Label(header_frame, text=f"--- {key} ---", font=("TkDefaultFont", 11, "bold"),
                                      bg="#f5f5f5")
                    header.pack(side="left", padx=padx_label, pady=(6, 6))
                    self.current_row += 1
                    self.create_entry_widgets(parent, val, prefix=full_key, indent_level=indent_level + 1)
            # Handle other data types
            else:
                comment = self.get_comment_for_path(prefix, key)
                icon = ""
                if comment:
                    m = re.search(r'\(([^)]{1,6})\)', comment)
                    if m:
                        icon = m.group(1)

                label_text = f"{icon} {key}" if icon else key

                row_frame = tk.Frame(parent, bg=bg_color)
                row_frame.grid(row=self.current_row, column=0, columnspan=2, sticky="ew")

                lbl = tk.Label(row_frame, text=label_text, anchor="w", justify="left", bg=bg_color)
                lbl.pack(side="left", padx=padx_label, pady=2, expand=True, fill="x")

                orig_val = self._get_original_value(full_key)

                if isinstance(orig_val, bool):
                    var = tk.BooleanVar(value=orig_val)
                    cb = tk.Checkbutton(row_frame, variable=var, bg=bg_color)
                    cb.pack(side="right", padx=6)
                    self.widgets[full_key] = var
                elif isinstance(orig_val, list):
                    text = tk.Text(row_frame, height=4, width=60, wrap=tk.WORD)
                    if orig_val:
                        text.insert("1.0", "\n".join(str(x) for x in orig_val))
                    text.pack(side="right", padx=6)
                    self.widgets[full_key] = text
                else:
                    entry = tk.Entry(row_frame, width=60)
                    val_to_show = self._get_current_value_for_display(full_key)
                    entry.insert(0, "" if val_to_show is None else str(val_to_show))
                    entry.pack(side="right", padx=6)
                    self.widgets[full_key] = entry

                self.current_row += 1

    def _get_original_value(self, full_key):
        """Gets the type and value from self.original_values by full_key path"""
        parts = full_key.split(".")
        cur = self.original_values
        for p in parts:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(p)
            if cur is None:
                return None
        return cur

    def _get_current_value_for_display(self, full_key):
        """Similar to _get_original_value but looks in self.ordered_config (after merge)"""
        parts = full_key.split(".")
        cur = self.ordered_config
        for p in parts:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(p)
            if cur is None:
                return None
        return cur

    # New helper function to convert OrderedDict to a regular dict recursively
    def _to_plain_dict(self, data):
        if isinstance(data, OrderedDict):
            return {k: self._to_plain_dict(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._to_plain_dict(item) for item in data]
        else:
            return data

    def collect_values(self):
        """
        Collects values from self.widgets into a regular nested dict structure,
        using the key order from self.ordered_config
        """

        def recurse(template, prefix=""):
            out = OrderedDict()
            for k, v in template.items():
                full_key = f"{prefix}.{k}" if prefix else k
                orig_val = self._get_original_value(full_key)

                # If it's the "char" + "value" structure, we only collect 'value'
                if isinstance(orig_val, dict) and 'char' in orig_val and 'value' in orig_val:
                    widget = self.widgets.get(f"{full_key}.value")
                    if isinstance(widget, tk.Entry):
                        s = widget.get().strip()
                        try:
                            # Try to convert to a number if the original was a number
                            out[k] = {'char': orig_val['char'], 'value': float(s)}
                        except (ValueError, TypeError):
                            out[k] = {'char': orig_val['char'], 'value': s}
                    continue

                if isinstance(orig_val, dict):
                    out[k] = recurse(v, full_key)
                    continue

                widget = self.widgets.get(full_key)
                if isinstance(widget, tk.BooleanVar):
                    out[k] = bool(widget.get())
                elif isinstance(widget, tk.Text):
                    raw = widget.get("1.0", tk.END).strip()
                    out[k] = [line for line in (raw.splitlines()) if line.strip()]
                elif isinstance(widget, tk.Entry):
                    s = widget.get().strip()
                    if s == "":
                        out[k] = None
                    elif orig_val is None or isinstance(orig_val, str):
                        out[k] = s
                    elif isinstance(orig_val, int):
                        try:
                            out[k] = int(s)
                        except (ValueError, TypeError):
                            messagebox.showwarning("Type Error",
                                                   f"Value for '{full_key}' should be an integer. Saved as a string.")
                            out[k] = s
                    elif isinstance(orig_val, float):
                        try:
                            out[k] = float(s)
                        except (ValueError, TypeError):
                            messagebox.showwarning("Type Error",
                                                   f"Value for '{full_key}' should be a float. Saved as a string.")
                            out[k] = s
                    else:
                        out[k] = s
                else:
                    out[k] = self._get_current_value_for_display(full_key)
            return out

        return recurse(self.ordered_config, "")

    def save_config(self):
        new_data = self.collect_values()

        # We no longer need _to_plain_dict because we registered a representer for OrderedDict
        # The dump function will now handle it correctly.

        try:
            with open(self.config_file_path, "w", encoding="utf-8") as f:
                yaml.dump(new_data, f, allow_unicode=True, sort_keys=False)
            self.destroy()  # Closing the application window after saving
        except Exception as e:
            messagebox.showerror("Error", f"Could not save file: {e}")


# ---------- Launch ----------
if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, "config.yaml")

    app = ConfigEditor(config_file)
    app.mainloop()
