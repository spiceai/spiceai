import os
import shutil
from prompt_toolkit import Application
from prompt_toolkit.layout import Layout, HSplit, Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.styles import Style
from prompt_toolkit.widgets import Frame
from prompt_toolkit.layout.dimension import Dimension
from prompt_toolkit.layout import ScrollablePane


def list_config_files():
    """Scan the current directory for configuration files."""
    return [
        f
        for f in os.listdir(".")
        if (f.endswith("spicepod.yaml") or f.endswith("spicepod.yml"))
        and (f != "spicepod.yaml" and f != "spicepod.yml")
    ]


def interactive_file_selector(files):
    """Interactive selection of a configuration file."""
    selected_index = [0]  # Mutable integer to track the index

    # Define key bindings
    kb = KeyBindings()

    @kb.add("down")
    def _down(event):
        """Move cursor down."""
        if selected_index[0] < len(files) - 1:
            selected_index[0] += 1
            update_view(event.app)

    @kb.add("up")
    def _up(event):
        """Move cursor up."""
        if selected_index[0] > 0:
            selected_index[0] -= 1
            update_view(event.app)

    @kb.add("enter")
    def _enter(event):
        """Select the highlighted file."""
        event.app.exit(result=files[selected_index[0]])

    @kb.add("c-c")
    @kb.add("escape")
    def _exit(event):
        """Exit the application."""
        event.app.exit(result=None)

    # Styling applied to highlight selected options
    style = Style.from_dict(
        {
            "selected": "reverse",
        }
    )

    def get_formatted_text():
        """Generate text for each file with the selected one highlighted."""
        result = []
        for i, file in enumerate(files):
            if i == selected_index[0]:
                result.append(("class:selected", f" {file}"))
            else:
                result.append(("", f" {file}"))
            result.append(("", "\n"))
        return result

    # Create a formatted text control
    content_control = FormattedTextControl(get_formatted_text)
    window = Window(content=content_control, wrap_lines=False)

    # Setup for scrollable pane with a fixed height limit
    scrollable_pane = ScrollablePane(
        content=HSplit([window]),
        height=Dimension(min=5, max=15),
        keep_focused_window_visible=True,
    )

    def update_view(app):
        """Ensure focused item is in the visible window by adjusting scroll."""
        pane_height = 15  # Fixed height based on constraint
        vertical_scroll = scrollable_pane.vertical_scroll
        if selected_index[0] >= vertical_scroll + pane_height:
            scrollable_pane.vertical_scroll = selected_index[0] - (pane_height - 1)
        elif selected_index[0] < vertical_scroll:
            scrollable_pane.vertical_scroll = selected_index[0]

    # Application setup
    app = Application(
        layout=Layout(
            HSplit(
                [
                    Frame(scrollable_pane, title="Select Configuration File"),
                ]
            )
        ),
        key_bindings=kb,
        full_screen=True,
        style=style,
    )
    return app.run()


def switch_config_file(selected_file):
    """Rename the selected configuration file to 'spicepod.yaml'."""
    if not selected_file:
        print("No file selected to switch.")
        return

    destination_file = "spicepod.yaml"
    try:
        shutil.copyfile(selected_file, destination_file)
        print(
            f"Switched configuration to {selected_file} and renamed it to {destination_file}."
        )
    except IOError as e:
        print(f"An error occurred while switching configuration: {e}")


def main():
    try:
        config_files = list_config_files()
        if not config_files:
            print("No configuration files found.")
            return

        selected_file = interactive_file_selector(config_files)
        switch_config_file(selected_file)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
