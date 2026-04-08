
def tournament(candidates):
    if not candidates:
        return None

    return sorted(
        candidates,
        key=lambda c: (c.get("stability",0), c.get("score",0)),
        reverse=True
    )[0]
