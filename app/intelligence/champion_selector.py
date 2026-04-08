
def select_champion(strategies):
    if not strategies:
        return None

    return sorted(
        strategies,
        key=lambda s: (s.get("stability",0), s.get("score",0)),
        reverse=True
    )[0]
