from __future__ import annotations


from app.engine.promotion_engine import PromotionEngine


def test_promotion_engine_has_self_learning_hooks():
    engine = PromotionEngine()
    engine.review_champions({})
    engine.evaluate_candidates({})

