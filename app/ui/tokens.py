# theme.py - Layout, Color, and Style Tokens

# ========================
# LAYOUT TOKENS
# ========================
LAYOUT = {
    # Spacing (in pixels)
    "spacing_xxs": 2,
    "spacing_xs": 4,
    "spacing_sm": 8,
    "spacing_md": 16,
    "spacing_lg": 24,
    "spacing_xl": 32,
    "spacing_xxl": 48,
    
    # Border Radius
    "radius_sm": 4,
    "radius_md": 8,
    "radius_lg": 12,
    "radius_xl": 16,
    "radius_full": 9999,
    
    # Component Sizes
    "button_height_sm": 32,
    "button_height_md": 40,
    "button_height_lg": 48,
    
    "input_height_sm": 32,
    "input_height_md": 40,
    "input_height_lg": 48,
    
    # Sidebar
    "sidebar_width": 240,
    
    # Container Widths
    "container_sm": 480,
    "container_md": 768,
    "container_lg": 1024,
    "container_xl": 1280,
    
    # Z-Index Scale
    "z_dropdown": 100,
    "z_sticky": 200,
    "z_modal": 300,
    "z_popover": 400,
    "z_tooltip": 500,
}

# ========================
# COLOR TOKENS
# ========================
COLORS = {
    # Primary Colors
    "primary_50": "#e6f2ff",
    "primary_100": "#cce5ff",
    "primary_200": "#99ccff",
    "primary_300": "#66b3ff",
    "primary_400": "#3399ff",
    "primary_500": "#007bff",  # Main primary
    "primary_600": "#0069d9",
    "primary_700": "#0052a3",
    "primary_800": "#003d7a",
    "primary_900": "#002952",
    
    # Secondary Colors
    "secondary_50": "#f0f0f0",
    "secondary_100": "#d9d9d9",
    "secondary_200": "#bfbfbf",
    "secondary_300": "#a6a6a6",
    "secondary_400": "#8c8c8c",
    "secondary_500": "#737373",  # Main secondary
    "secondary_600": "#595959",
    "secondary_700": "#404040",
    "secondary_800": "#262626",
    "secondary_900": "#0d0d0d",
    
    # Accent Colors
    "accent_success": "#28a745",
    "accent_warning": "#ffc107",
    "accent_error": "#dc3545",
    "accent_info": "#17a2b8",
    
    # Neutral Colors
    "white": "#ffffff",
    "gray_50": "#f8f9fa",
    "gray_100": "#f1f3f5",
    "gray_200": "#e9ecef",
    "gray_300": "#dee2e6",
    "gray_400": "#ced4da",
    "gray_500": "#adb5bd",
    "gray_600": "#6c757d",
    "gray_700": "#495057",
    "gray_800": "#343a40",
    "gray_900": "#212529",
    "black": "#000000",
    
    # Semantic Colors
    "bg_primary": "#ffffff",
    "bg_secondary": "#f8f9fa",
    "bg_tertiary": "#e9ecef",
    
    "text_primary": "#212529",
    "text_secondary": "#6c757d",
    "text_disabled": "#adb5bd",
    
    "border_default": "#dee2e6",
    "border_focus": "#007bff",
}

# Example with a button
button_style = {
    **STYLES["button_primary"],
    "padding": f"{LAYOUT['spacing_sm']} {LAYOUT['spacing_lg']}",
}

# Example with a card
card = {
    **STYLES["card_elevated"],
    "margin": LAYOUT["spacing_md"],
}

# ========================
# TYPOGRAPHY (Existing + New)
# ========================
TYPO = {
    "display": 32,
    "title": 20,
    "body": 14,
    "caption": 12,
    "label": 14,
    "button": 14,
    
    # Font Weights
    "weight_light": 300,
    "weight_regular": 400,
    "weight_medium": 500,
    "weight_semibold": 600,
    "weight_bold": 700,
    
    # Line Heights
    "line_height_tight": 1.2,
    "line_height_normal": 1.5,
    "line_height_relaxed": 1.75,
}

# ========================
# STYLE PRESETS
# ========================
STYLES = {
    # Primary Button
    "button_primary": {
        "background_color": COLORS["primary_500"],
        "color": COLORS["white"],
        "border_radius": LAYOUT["radius_md"],
        "padding": f"{LAYOUT['spacing_sm']} {LAYOUT['spacing_md']}",
        "font_size": TYPO["button"],
        "font_weight": TYPO["weight_medium"],
        "border": "none",
        "cursor": "pointer",
    },

    # Secondary Button
    "button_secondary": {
        "background_color": "transparent",
        "color": COLORS["primary_500"],
        "border_radius": LAYOUT["radius_md"],
        "padding": f"{LAYOUT['spacing_sm']} {LAYOUT['spacing_md']}",
        "font_size": TYPO["button"],
        "font_weight": TYPO["weight_medium"],
        "border": f"1px solid {COLORS['primary_500']}",
        "cursor": "pointer",
    },

    # Danger Button
    "button_danger": {
        "background_color": COLORS["accent_error"],
        "color": COLORS["white"],
        "border_radius": LAYOUT["radius_md"],
        "padding": f"{LAYOUT['spacing_sm']} {LAYOUT['spacing_md']}",
        "font_size": TYPO["button"],
        "font_weight": TYPO["weight_medium"],
        "border": "none",
        "cursor": "pointer",
    },
}


# ========================
# USAGE EXAMPLES
# ========================
from app.ui.theme import LAYOUT, COLORS, TYPO, STYLES

# spacing
margin = LAYOUT["spacing_md"]

# color
button_bg = COLORS["primary_500"]

# preset
button_style = STYLES["button_primary"]
secondary_style = STYLES["button_secondary"]
danger_style = STYLES["button_danger"]