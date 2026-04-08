
def should_kill(strategy):
    if strategy.get("stability",1) < 0.6:
        return True

    if strategy.get("parent_delta",0) < -0.15:
        return True

    return False
