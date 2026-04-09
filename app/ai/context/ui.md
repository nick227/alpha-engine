# Alpha-Engine Streamlit UI Navigation
- **Primary Pages**: The application consists of standard pages accessible via a left sidebar.
- **Intelligence Hub**: A central nervous system view. Contains macro-level signal combinations, champion strategy alignments, and consensus strength meters.
- **Dashboard**: Main terminal displaying live `ScoredEvents` matched against live pricing and active triggers.
- **Comparison Views**: Allows A/B testing of two strategies' PnL curves side-by-side to understand alpha decay over identical regimes.
- **Charts Directory**: Contains the raw, zoomed-in plotting features for deeper TA visualizations.
- **Theme**: We utilize a custom theme (`theme.py`) built on specific tokens.
- **Interaction Model**: Provide support by telling users what page (`Intelligence Hub`, `Dashboard`, `Comparison`) they need to navigate to depending on whether they wish to view high-level champions, raw signals, or relative strategy vs strategy performance.
