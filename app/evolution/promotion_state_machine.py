
class PromotionStateMachine:

    def candidate(self, s):
        s["status"] = "CANDIDATE"
        return s

    def probation(self, s):
        s["status"] = "PROBATION"
        return s

    def activate(self, s):
        s["status"] = "ACTIVE"
        return s

    def rollback(self, s):
        s["status"] = "ROLLED_BACK"
        return s

    def archive(self, s):
        s["status"] = "ARCHIVED"
        return s
