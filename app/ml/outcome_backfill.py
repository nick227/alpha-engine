"""
Automatic outcome backfill for dimensional ML system.
Fills actual_return for predictions that have matured.
"""

import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import yfinance as yf


class OutcomeBackfill:
    """Automatically fills actual outcomes for mature predictions."""
    
    def __init__(self, db_path: str = "data/alpha.db"):
        self.db_path = db_path
    
    def get_predictions_without_outcomes(self, horizon_days: int = 7) -> List[Dict[str, Any]]:
        """Get predictions that are mature but don't have outcomes yet."""
        conn = sqlite3.connect(self.db_path)
        try:
            # Get mature predictions without actual_return
            cursor = conn.execute("""
                SELECT 
                    id,
                    symbol,
                    prediction_date,
                    prediction,
                    confidence,
                    axis_key,
                    horizon_tag
                FROM dimensional_predictions 
                WHERE actual_return IS NULL 
                AND matured = TRUE
                ORDER BY prediction_date ASC
                LIMIT 100
            """)
            
            rows = cursor.fetchall()
            
            predictions = []
            for row in rows:
                predictions.append({
                    'id': row[0],
                    'symbol': row[1],
                    'prediction_date': row[2],
                    'prediction': row[3],
                    'confidence': row[4],
                    'axis_key': row[5],
                    'horizon_tag': row[6]
                })
            
            return predictions
            
        except Exception as e:
            print(f"Error getting predictions without outcomes: {e}")
            return []
        finally:
            conn.close()
    
    def mark_mature_predictions(self, horizon_days: int = 7) -> int:
        """Mark predictions as mature when they reach their horizon."""
        conn = sqlite3.connect(self.db_path)
        try:
            # Mark predictions that have reached their horizon
            cursor = conn.execute("""
                UPDATE dimensional_predictions 
                SET matured = TRUE 
                WHERE matured = FALSE 
                AND prediction_date <= DATE('now', '-' || ? || ' day')
            """, (horizon_days,))
            
            updated_count = cursor.rowcount
            conn.commit()
            
            return updated_count
            
        except Exception as e:
            print(f"Error marking mature predictions: {e}")
            return 0
        finally:
            conn.close()
    
    def get_daily_maturity_stats(self) -> Dict[str, int]:
        """Get daily maturity statistics for monitoring."""
        conn = sqlite3.connect(self.db_path)
        try:
            # Get predictions that matured today
            cursor = conn.execute("""
                SELECT COUNT(*) 
                FROM dimensional_predictions 
                WHERE matured = TRUE 
                AND actual_return IS NULL
                AND DATE(prediction_date, '+' || horizon_tag || ' day') = DATE('now')
            """)
            
            newly_matured_today = cursor.fetchone()[0]
            
            # Get total matured but unfilled
            cursor = conn.execute("""
                SELECT COUNT(*) 
                FROM dimensional_predictions 
                WHERE matured = TRUE 
                AND actual_return IS NULL
            """)
            
            total_matured_unfilled = cursor.fetchone()[0]
            
            return {
                "newly_matured_today": newly_matured_today,
                "total_matured_unfilled": total_matured_unfilled
            }
            
        except Exception as e:
            print(f"Error getting daily maturity stats: {e}")
            return {"newly_matured_today": 0, "total_matured_unfilled": 0}
        finally:
            conn.close()
    
    def get_price_at_date(self, symbol: str, date_str: str) -> Optional[float]:
        """Get stock price at specific date using yfinance."""
        try:
            # Convert date string to datetime
            target_date = datetime.strptime(date_str, '%Y-%m-%d')
            
            # Get ticker data with extended history
            ticker = yf.Ticker(symbol)
            
            # Get historical data for the date range
            start_date = target_date.strftime('%Y-%m-%d')
            end_date = (target_date + timedelta(days=5)).strftime('%Y-%m-%d')
            
            hist = ticker.history(start=start_date, end=end_date)
            
            if hist.empty:
                print(f"No price data found for {symbol} on {date_str}")
                return None
            
            # Get the closing price on or after the target date
            for date, row in hist.iterrows():
                return float(row['Close'])
            
            return None
            
        except Exception as e:
            print(f"Error getting price for {symbol} on {date_str}: {e}")
            return None
    
    def calculate_actual_return(self, symbol: str, prediction_date: str, horizon_days: int = 7) -> Optional[float]:
        """Calculate actual return from prediction date to horizon."""
        try:
            # Get price at prediction date
            start_price = self.get_price_at_date(symbol, prediction_date)
            if start_price is None:
                return None
            
            # Calculate target date
            target_date = datetime.strptime(prediction_date, '%Y-%m-%d') + timedelta(days=horizon_days)
            target_date_str = target_date.strftime('%Y-%m-%d')
            
            # Get price at target date
            end_price = self.get_price_at_date(symbol, target_date_str)
            if end_price is None:
                return None
            
            # Calculate return
            actual_return = (end_price / start_price) - 1.0
            
            return actual_return
            
        except Exception as e:
            print(f"Error calculating return for {symbol}: {e}")
            return None
    
    def update_prediction_outcome(self, prediction_id: int, actual_return: float) -> bool:
        """Update a single prediction with its actual outcome."""
        conn = sqlite3.connect(self.db_path)
        try:
            # Calculate prediction error
            cursor = conn.execute("SELECT prediction FROM dimensional_predictions WHERE id = ?", (prediction_id,))
            result = cursor.fetchone()
            if not result:
                return False
            
            prediction = result[0]
            prediction_error = abs(prediction - actual_return)
            
            # Update the record
            conn.execute("""
                UPDATE dimensional_predictions 
                SET actual_return = ?, prediction_error = ?
                WHERE id = ?
            """, (actual_return, prediction_error, prediction_id))
            
            conn.commit()
            return True
            
        except Exception as e:
            print(f"Error updating prediction {prediction_id}: {e}")
            return False
        finally:
            conn.close()
    
    def backfill_outcomes(self, horizon_days: int = 7) -> Dict[str, int]:
        """Main backfill function - fills outcomes for mature predictions."""
        print(f"=== OUTCOME BACKFILL START ===")
        
        # Step 1: Get daily maturity stats
        maturity_stats = self.get_daily_maturity_stats()
        newly_matured = maturity_stats["newly_matured_today"]
        total_unfilled = maturity_stats["total_matured_unfilled"]
        
        print(f"Daily maturity: {newly_matured} newly matured, {total_unfilled} total unfilled")
        
        # Step 2: Mark mature predictions
        matured_count = self.mark_mature_predictions(horizon_days)
        print(f"Marked {matured_count} predictions as mature")
        
        # Step 3: Get predictions that need outcomes
        predictions = self.get_predictions_without_outcomes(horizon_days)
        print(f"Found {len(predictions)} mature predictions needing outcomes")
        
        if not predictions:
            missed = newly_matured - 0  # All newly matured were processed
            print(f"=== BACKFILL COMPLETE ===")
            print(f"Matured today: {newly_matured}, Backfilled: 0, Missed: {missed}")
            return {"processed": 0, "updated": 0, "failed": 0, "matured": matured_count, "newly_matured": newly_matured, "missed": missed}
        
        processed = 0
        updated = 0
        failed = 0
        
        for pred in predictions:
            processed += 1
            
            # Calculate actual return
            actual_return = self.calculate_actual_return(
                pred['symbol'], 
                pred['prediction_date'], 
                horizon_days
            )
            
            if actual_return is not None:
                # Update prediction
                if self.update_prediction_outcome(pred['id'], actual_return):
                    updated += 1
                    print(f"✅ Updated {pred['symbol']}: {pred['prediction']:.3f} → {actual_return:.3f}")
                else:
                    failed += 1
                    print(f"❌ Failed to update {pred['symbol']}")
            else:
                failed += 1
                print(f"❌ No price data for {pred['symbol']}")
        
        # Calculate missed predictions
        missed = newly_matured - updated
        
        print(f"=== BACKFILL COMPLETE ===")
        print(f"Matured today: {newly_matured}, Backfilled: {updated}, Missed: {missed}")
        
        return {"processed": processed, "updated": updated, "failed": failed, "matured": matured_count, "newly_matured": newly_matured, "missed": missed}
    
    def get_outcome_statistics(self) -> Dict[str, Any]:
        """Get statistics on outcome coverage."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_predictions,
                    COUNT(CASE WHEN actual_return IS NOT NULL THEN 1 ELSE NULL END) as predictions_with_outcomes,
                    AVG(CASE WHEN actual_return IS NOT NULL THEN actual_return ELSE NULL END) as avg_actual_return,
                    COUNT(CASE WHEN actual_return > 0 AND actual_return IS NOT NULL THEN 1 ELSE NULL END) as winning_predictions,
                    COUNT(DISTINCT axis_key) as total_axes,
                    COUNT(DISTINCT CASE WHEN actual_return IS NOT NULL THEN axis_key ELSE NULL END) as axes_with_outcomes
                FROM dimensional_predictions
            """)
            
            stats = cursor.fetchone()
            total, with_outcomes, avg_return, wins, total_axes, axes_with_outcomes = stats
            
            win_rate = wins / with_outcomes if with_outcomes > 0 else 0.0
            outcome_coverage = with_outcomes / total if total > 0 else 0.0
            
            return {
                "total_predictions": total,
                "predictions_with_outcomes": with_outcomes,
                "outcome_coverage": outcome_coverage,
                "avg_actual_return": avg_return,
                "winning_predictions": wins,
                "win_rate": win_rate,
                "total_axes": total_axes,
                "axes_with_outcomes": axes_with_outcomes
            }
            
        except Exception as e:
            return {"error": str(e)}
        finally:
            conn.close()


def run_backfill():
    """Run the backfill process."""
    backfill = OutcomeBackfill()
    
    # Get current stats
    print("Current outcome statistics:")
    stats = backfill.get_outcome_statistics()
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.3f}")
        else:
            print(f"  {key}: {value}")
    
    print()
    
    # Run backfill
    result = backfill.backfill_outcomes()
    
    print()
    print("Updated outcome statistics:")
    stats = backfill.get_outcome_statistics()
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.3f}")
        else:
            print(f"  {key}: {value}")


if __name__ == "__main__":
    run_backfill()
