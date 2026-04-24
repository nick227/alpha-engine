"""
Enhanced UI Components for Alpha Engine Dashboard
Sophisticated components using the enhanced theme system
"""

import streamlit as st
import plotly.graph_objects as go
from typing import Any, Optional, Dict, List
from app.ui.theme import COLORS, TYPOGRAPHY, SPACING, LAYOUT, get_color_for_direction


def elevated_card(title: str = None, content: str = None, footer: str = None, 
                border_radius: str = None, shadow: bool = True, icon: str = None):
    """
    Modern elevated card with sophisticated styling
    
    Args:
        title: Card title
        content: Main content (HTML string)
        footer: Footer content
        border_radius: Custom border radius
        shadow: Whether to show shadow
        icon: Optional icon emoji
    """
    radius = border_radius or LAYOUT["radius_lg"]
    shadow_css = LAYOUT["shadow_md"] if shadow else "none"
    
    title_html = ""
    if title or icon:
        icon_html = f"{icon} " if icon else ""
        title_html = f"""
        <div style="
            margin: 0 0 {SPACING['4']} 0; 
            font-size: {TYPOGRAPHY['text_xl']}; 
            font-weight: {TYPOGRAPHY['weight_semibold']}; 
            color: {COLORS['neutral_900']};
            display: flex;
            align-items: center;
        ">
            {icon_html}{title}
        </div>
        """
    
    content_html = ""
    if content:
        content_html = f"""
        <div style="
            color: {COLORS['neutral_700']}; 
            line-height: {TYPOGRAPHY['leading_normal']};
            font-size: {TYPOGRAPHY['text_base']};
        ">
            {content}
        </div>
        """
    
    footer_html = ""
    if footer:
        footer_html = f"""
        <div style="
            margin-top: {SPACING['4']}; 
            padding-top: {SPACING['4']}; 
            border-top: 1px solid {COLORS['neutral_200']};
            font-size: {TYPOGRAPHY['text_sm']};
            color: {COLORS['neutral_600']};
        ">
            {footer}
        </div>
        """
    
    card_html = f"""
    <div style="
        background: {COLORS['surface']};
        border-radius: {radius};
        padding: {LAYOUT['card_padding']};
        margin-bottom: {SPACING['4']};
        box-shadow: {shadow_css};
        border: 1px solid {COLORS['border_light']};
    ">
        {title_html}
        {content_html}
        {footer_html}
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)


def metric_card(title: str, value: str, subtitle: str = None, 
               trend: float = None, color: str = None, icon: str = None):
    """
    Sophisticated metric card with trend indicators
    
    Args:
        title: Metric title
        value: Main metric value
        subtitle: Supporting subtitle
        trend: Trend percentage (positive/negative)
        color: Custom color override
        icon: Optional icon emoji
    """
    metric_color = color or COLORS['primary_800']
    
    # Format trend
    trend_html = ""
    if trend is not None:
        trend_icon = "↑" if trend > 0 else "↓" if trend < 0 else "→"
        trend_color = COLORS['success_700'] if trend > 0 else COLORS['error_700'] if trend < 0 else COLORS['neutral_600']
        trend_html = f"""
        <div style="
            font-size: {TYPOGRAPHY['text_sm']}; 
            color: {trend_color}; 
            font-weight: {TYPOGRAPHY['weight_medium']};
            margin-top: {SPACING['2']};
        ">
            {trend_icon} {abs(trend):.1f}%
        </div>
        """
    
    # Title
    title_html = f"""
    <div style="
        font-size: {TYPOGRAPHY['text_xs']}; 
        color: {COLORS['neutral_600']}; 
        margin-bottom: {SPACING['2']}; 
        text-transform: uppercase; 
        letter-spacing: 0.5px;
        display: flex;
        align-items: center;
        gap: {SPACING['1']};
    ">
        {f"<span>{icon}</span>" if icon else ""}{title}
    </div>
    """
    
    # Value
    value_html = f"""
    <div style="
        font-size: {TYPOGRAPHY['text_3xl']}; 
        font-weight: {TYPOGRAPHY['weight_bold']}; 
        color: {metric_color}; 
        margin-bottom: {SPACING['1']};
        line-height: {TYPOGRAPHY['leading_tight']};
    ">
        {value}
    </div>
    """
    
    # Subtitle
    subtitle_html = ""
    if subtitle:
        subtitle_html = f"""
        <div style="
            font-size: {TYPOGRAPHY['text_sm']}; 
            color: {COLORS['neutral_600']};
        ">
            {subtitle}
        </div>
        """
    
    card_html = f"""
    <div style="
        background: {COLORS['surface']};
        border-radius: {LAYOUT['radius_lg']};
        padding: {SPACING['5']};
        border: 1px solid {COLORS['border_light']};
        box-shadow: {LAYOUT['shadow_sm']};
        text-align: left;
    ">
        {title_html}
        {value_html}
        {subtitle_html}
        {trend_html}
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)


def strategy_metric_card(strategy: Any, strategy_type: str = "primary"):
    """
    Specialized metric card for strategy information
    
    Args:
        strategy: Strategy object with metrics
        strategy_type: Type of strategy (affects styling)
    """
    if not strategy:
        empty_html = f"""
        <div style="
            background: {COLORS['surface']};
            border-radius: {LAYOUT['radius_lg']};
            padding: {SPACING['5']};
            border: 1px solid {COLORS['border_light']};
            box-shadow: {LAYOUT['shadow_sm']};
            text-align: center;
            color: {COLORS['neutral_500']};
            font-style: italic;
        ">
            No data available
        </div>
        """
        st.markdown(empty_html, unsafe_allow_html=True)
        return
    
    # Determine color based on strategy type
    if 'sentiment' in strategy_type.lower():
        color = COLORS['sentiment_positive']
        icon = "📊"
    elif 'quant' in strategy_type.lower():
        color = COLORS['quant_primary']
        icon = "🔬"
    elif 'consensus' in strategy_type.lower():
        color = COLORS['consensus_primary']
        icon = "🤝"
    else:
        color = COLORS['primary_800']
        icon = "📈"
    
    # Create content
    content_html = f"""
    <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: {SPACING['3']};">
        <div>
            <div style="
                font-size: {TYPOGRAPHY['text_lg']}; 
                font-weight: {TYPOGRAPHY['weight_semibold']}; 
                color: {color};
                margin-bottom: {SPACING['1']};
            ">
                {icon} {strategy.strategy_id}
            </div>
            <div style="
                font-size: {TYPOGRAPHY['text_sm']}; 
                color: {COLORS['neutral_600']};
                font-style: italic;
            ">
                {strategy_type.title()}
            </div>
        </div>
        <div style="
            background: {color}20;
            color: {color};
            padding: {SPACING['1']} {SPACING['3']};
            border-radius: {LAYOUT['radius_full']};
            font-size: {TYPOGRAPHY['text_xs']};
            font-weight: {TYPOGRAPHY['weight_medium']};
        ">
            cw {strategy.confidence_weight:.2f}
        </div>
    </div>
    
    <div style="
        display: grid; 
        grid-template-columns: 1fr 1fr 1fr; 
        gap: {SPACING['3']};
        margin-top: {SPACING['3']};
    ">
        <div style="text-align: center;">
            <div style="font-size: {TYPOGRAPHY['text_lg']}; font-weight: {TYPOGRAPHY['weight_bold']}; color: {COLORS['neutral_900']};">
                {strategy.win_rate:.2f}
            </div>
            <div style="font-size: {TYPOGRAPHY['text_xs']}; color: {COLORS['neutral_600']};">Win Rate</div>
        </div>
        <div style="text-align: center;">
            <div style="font-size: {TYPOGRAPHY['text_lg']}; font-weight: {TYPOGRAPHY['weight_bold']}; color: {COLORS['neutral_900']};">
                {strategy.alpha:.4f}
            </div>
            <div style="font-size: {TYPOGRAPHY['text_xs']}; color: {COLORS['neutral_600']};">Alpha</div>
        </div>
        <div style="text-align: center;">
            <div style="font-size: {TYPOGRAPHY['text_lg']}; font-weight: {TYPOGRAPHY['weight_bold']}; color: {COLORS['neutral_900']};">
                {strategy.stability:.2f}
            </div>
            <div style="font-size: {TYPOGRAPHY['text_xs']}; color: {COLORS['neutral_600']};">Stability</div>
        </div>
    </div>
    """
    
    card_html = f"""
    <div style="
        background: {COLORS['surface']};
        border-radius: {LAYOUT['radius_lg']};
        padding: {SPACING['5']};
        border: 1px solid {COLORS['border_light']};
        box-shadow: {LAYOUT['shadow_sm']};
    ">
        {content_html}
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)


def signal_indicator(direction: str, confidence: float, strategy: str = None):
    """
    Visual signal indicator with direction and confidence
    
    Args:
        direction: Signal direction (up/down/neutral)
        confidence: Confidence score (0-1)
        strategy: Strategy name
    """
    color = get_color_for_direction(direction)
    direction_icon = "↑" if direction in ('up', 'long', 'buy', '1', '+1') else "↓" if direction in ('down', 'short', 'sell', '-1') else "→"
    
    # Confidence bar
    confidence_width = confidence * 100
    confidence_color = COLORS['success_500'] if confidence > 0.7 else COLORS['warning_500'] if confidence > 0.4 else COLORS['error_500']
    
    indicator_html = f"""
    <div style="
        display: flex;
        align-items: center;
        gap: {SPACING['3']};
        padding: {SPACING['3']};
        background: {COLORS['surface']};
        border-radius: {LAYOUT['radius_md']};
        border: 1px solid {COLORS['border_light']};
    ">
        <div style="
            width: 32px;
            height: 32px;
            border-radius: 50%;
            background: {color}20;
            color: {color};
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: {TYPOGRAPHY['weight_bold']};
            font-size: {TYPOGRAPHY['text_lg']};
        ">
            {direction_icon}
        </div>
        <div style="flex: 1;">
            {f'<div style="font-size: {TYPOGRAPHY["text_xs"]}; color: {COLORS["neutral_600"]}; margin-bottom: 2px;">{strategy}</div>' if strategy else ''}
            <div style="font-size: {TYPOGRAPHY['text_sm']}; color: {COLORS['neutral_700']};">
                Confidence: {confidence:.3f}
            </div>
            <div style="
                width: 100%;
                height: 4px;
                background: {COLORS['neutral_200']};
                border-radius: {LAYOUT['radius_full']};
                margin-top: {SPACING['2']};
                overflow: hidden;
            ">
                <div style="
                    width: {confidence_width}%;
                    height: 100%;
                    background: {confidence_color};
                    border-radius: {LAYOUT['radius_full']};
                    transition: width 0.3s ease;
                "></div>
            </div>
        </div>
    </div>
    """
    st.markdown(indicator_html, unsafe_allow_html=True)


def status_badge(status: str, size: str = "md"):
    """
    Status badge with appropriate styling
    
    Args:
        status: Status text
        size: Badge size (sm, md, lg)
    """
    status_colors = {
        'healthy': COLORS['success_500'],
        'warning': COLORS['warning_500'],
        'error': COLORS['error_500'],
        'unknown': COLORS['neutral_500'],
        'active': COLORS['success_500'],
        'inactive': COLORS['neutral_500'],
        'pending': COLORS['warning_500'],
    }
    
    sizes = {
        'sm': {'padding': '2px 6px', 'font_size': '10px'},
        'md': {'padding': '4px 8px', 'font_size': '12px'},
        'lg': {'padding': '6px 12px', 'font_size': '14px'},
    }
    
    color = status_colors.get(status.lower(), COLORS['neutral_500'])
    size_config = sizes.get(size, sizes['md'])
    
    badge_html = f"""
    <span style="
        display: inline-block;
        padding: {size_config['padding']};
        background: {color}20;
        color: {color};
        border-radius: {LAYOUT['radius_full']};
        font-size: {size_config['font_size']};
        font-weight: {TYPOGRAPHY['weight_medium']};
        text-transform: uppercase;
        letter-spacing: 0.5px;
        border: 1px solid {color}40;
    ">
        {status}
    </span>
    """
    return badge_html


def divider(title: str = None, spacing: str = "md"):
    """
    Sophisticated section divider
    
    Args:
        title: Optional title for the divider
        spacing: Spacing size
    """
    spacing_value = SPACING.get(spacing, SPACING['4'])
    
    if title:
        divider_html = f"""
        <div style="
            display: flex;
            align-items: center;
            margin: {spacing_value} 0;
        ">
            <div style="
                flex: 1;
                height: 1px;
                background: {COLORS['border_light']};
            "></div>
            <div style="
                padding: 0 {SPACING['4']};
                font-size: {TYPOGRAPHY['text_sm']};
                color: {COLORS['neutral_600']};
                font-weight: {TYPOGRAPHY['weight_medium']};
                text-transform: uppercase;
                letter-spacing: 0.5px;
            ">
                {title}
            </div>
            <div style="
                flex: 1;
                height: 1px;
                background: {COLORS['border_light']};
            "></div>
        </div>
        """
    else:
        divider_html = f"""
        <div style="
            height: 1px;
            background: {COLORS['border_light']};
            margin: {spacing_value} 0;
        "></div>
        """
    
    st.markdown(divider_html, unsafe_allow_html=True)


def info_panel(title: str, content: str, icon: str = "ℹ️", variant: str = "info"):
    """
    Information panel with icon and variant styling
    
    Args:
        title: Panel title
        content: Panel content
        icon: Icon emoji
        variant: Panel variant (info, warning, error, success)
    """
    variant_colors = {
        'info': {'bg': COLORS['info_50'], 'border': COLORS['info_200'], 'text': COLORS['info_700']},
        'warning': {'bg': COLORS['warning_50'], 'border': COLORS['warning_200'], 'text': COLORS['warning_700']},
        'error': {'bg': COLORS['error_50'], 'border': COLORS['error_200'], 'text': COLORS['error_700']},
        'success': {'bg': COLORS['success_50'], 'border': COLORS['success_200'], 'text': COLORS['success_700']},
    }
    
    colors = variant_colors.get(variant, variant_colors['info'])
    
    panel_html = f"""
    <div style="
        background: {colors['bg']};
        border: 1px solid {colors['border']};
        border-radius: {LAYOUT['radius_md']};
        padding: {SPACING['4']};
        margin-bottom: {SPACING['4']};
    ">
        <div style="
            display: flex;
            align-items: flex-start;
            gap: {SPACING['3']};
        ">
            <div style="font-size: {TYPOGRAPHY['text_lg']}; margin-top: 2px;">
                {icon}
            </div>
            <div style="flex: 1;">
                <div style="
                    font-size: {TYPOGRAPHY['text_sm']};
                    font-weight: {TYPOGRAPHY['weight_semibold']};
                    color: {colors['text']};
                    margin-bottom: {SPACING['2']};
                ">
                    {title}
                </div>
                <div style="
                    font-size: {TYPOGRAPHY['text_sm']};
                    color: {colors['text']};
                    line-height: {TYPOGRAPHY['leading_normal']};
                ">
                    {content}
                </div>
            </div>
        </div>
    </div>
    """
    st.markdown(panel_html, unsafe_allow_html=True)


def loading_skeleton(height: str = "200px", count: int = 1):
    """
    Loading skeleton placeholder
    
    Args:
        height: Height of each skeleton
        count: Number of skeletons to show
    """
    skeletons = []
    for i in range(count):
        skeleton_html = f"""
        <div style="
            height: {height};
            background: linear-gradient(90deg, {COLORS['neutral_200']} 25%, {COLORS['neutral_100']} 50%, {COLORS['neutral_200']} 75%);
            background-size: 200% 100%;
            animation: loading 1.5s infinite;
            border-radius: {LAYOUT['radius_md']};
            margin-bottom: {SPACING['4']};
        "></div>
        """
        skeletons.append(skeleton_html)
    
    # Add animation keyframes
    animation_css = """
    <style>
    @keyframes loading {
        0% { background-position: 200% 0; }
        100% { background-position: -200% 0; }
    }
    </style>
    """
    
    st.markdown(animation_css + "".join(skeletons), unsafe_allow_html=True)
