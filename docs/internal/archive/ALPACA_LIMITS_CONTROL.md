# Alpaca Built-in Limits - Location and Control

## Where the Limits Are Located

### **File Location**: `app/trading/providers/alpaca.py`

### **Exact Lines**: Lines 52-59 in the `submit_order` method

```python
# Enforce safety limits for testing
if quantity and quantity > 1.0:
    logger.warning(f"Quantity {quantity} exceeds test limit. Capping at 1.0 share.")
    quantity = 1.0

if notional and notional > 10.0:
    logger.warning(f"Notional ${notional} exceeds test limit. Capping at $10.0.")
    notional = 10.0
```

## How the Limits Work

### **1. Quantity Limit (1 share max)**
- **Location**: Line 53-55
- **Current limit**: `quantity > 1.0`
- **Action**: Caps at 1.0 share
- **Log message**: "Quantity {quantity} exceeds test limit. Capping at 1.0 share."

### **2. Notional Limit ($10 max)**
- **Location**: Line 57-59
- **Current limit**: `notional > 10.0`
- **Action**: Caps at $10.0
- **Log message**: "Notional ${notional} exceeds test limit. Capping at $10.0."

## How to Control the Limits

### **Option 1: Modify the Hard-coded Values**

Edit `app/trading/providers/alpaca.py` lines 53-59:

```python
# Enforce safety limits for testing
if quantity and quantity > 5.0:  # Changed from 1.0 to 5.0
    logger.warning(f"Quantity {quantity} exceeds test limit. Capping at 5.0 shares.")
    quantity = 5.0

if notional and notional > 100.0:  # Changed from 10.0 to 100.0
    logger.warning(f"Notional ${notional} exceeds test limit. Capping at $100.0.")
    notional = 100.0
```

### **Option 2: Make Limits Configurable**

Add configuration support to the AlpacaProvider:

#### **Step 1: Add to __init__ method**
```python
def __init__(self, api_key: str, api_secret: str, repository: AlphaRepository, 
             paper: bool = True, max_quantity: float = 1.0, max_notional: float = 10.0):
    self.client = TradingClient(api_key, api_secret, paper=paper)
    self.repository = repository
    self.paper = paper
    self.max_quantity = max_quantity
    self.max_notional = max_notional
    logger.info(f"AlpacaProvider initialized (paper={paper}, max_quantity={max_quantity}, max_notional=${max_notional})")
```

#### **Step 2: Update submit_order method**
```python
# Enforce safety limits for testing
if quantity and quantity > self.max_quantity:
    logger.warning(f"Quantity {quantity} exceeds test limit. Capping at {self.max_quantity} shares.")
    quantity = self.max_quantity

if notional and notional > self.max_notional:
    logger.warning(f"Notional ${notional} exceeds test limit. Capping at ${self.max_notional}.")
    notional = self.max_notional
```

#### **Step 3: Update initialization**
```python
# In your automation script
provider = AlpacaProvider(
    api_key, 
    api_secret, 
    repo, 
    paper=True,
    max_quantity=5.0,  # Allow up to 5 shares
    max_notional=100.0  # Allow up to $100
)
```

### **Option 3: Environment-based Limits**

Add environment variables to control limits:

#### **Step 1: Add to .env**
```bash
# Alpaca Trading Limits
ALPACA_MAX_QUANTITY=5.0
ALPACA_MAX_NOTIONAL=100.0
```

#### **Step 2: Update __init__ method**
```python
def __init__(self, api_key: str, api_secret: str, repository: AlphaRepository, paper: bool = True):
    self.client = TradingClient(api_key, api_secret, paper=paper)
    self.repository = repository
    self.paper = paper
    
    # Load limits from environment
    self.max_quantity = float(os.getenv('ALPACA_MAX_QUANTITY', '1.0'))
    self.max_notional = float(os.getenv('ALPACA_MAX_NOTIONAL', '10.0'))
    
    logger.info(f"AlpacaProvider initialized (paper={paper}, max_quantity={self.max_quantity}, max_notional=${self.max_notional})")
```

#### **Step 3: Update submit_order method**
```python
# Enforce safety limits for testing
if quantity and quantity > self.max_quantity:
    logger.warning(f"Quantity {quantity} exceeds test limit. Capping at {self.max_quantity} shares.")
    quantity = self.max_quantity

if notional and notional > self.max_notional:
    logger.warning(f"Notional ${notional} exceeds test limit. Capping at ${self.max_notional}.")
    notional = self.max_notional
```

## Current Limit Behavior

### **What Happens When Limits Are Exceeded**

1. **Warning logged**: The system logs a warning message
2. **Automatic capping**: The value is automatically reduced to the limit
3. **Order continues**: The order proceeds with the capped value
4. **No exception thrown**: The trade continues, just with reduced size

### **Example Scenarios**

```python
# Current limits: 1 share max, $10 max

# Scenario 1: Try to buy 5 shares
await provider.submit_order(
    ticker="SPY",
    quantity=5.0,  # Exceeds limit
    direction="long"
)
# Result: Logs warning, buys 1 share instead

# Scenario 2: Try to buy $50 worth
await provider.submit_order(
    ticker="SPY",
    notional=50.0,  # Exceeds limit
    direction="long"
)
# Result: Logs warning, buys $10 worth instead
```

## Safety Considerations

### **Why These Limits Exist**

1. **Paper trading safety**: Prevents accidental large positions
2. **Testing environment**: Keeps trades small for testing
3. **Cost control**: Limits potential losses
4. **API rate limiting**: Prevents excessive API calls

### **When to Increase Limits**

1. **Production testing**: Need realistic position sizes
2. **Strategy validation**: Testing with meaningful amounts
3. **Performance analysis**: Better P&L tracking with larger trades
4. **Risk management**: You have proper risk controls in place

### **Recommended Limit Progression**

```python
# Phase 1: Initial testing
max_quantity=1.0, max_notional=10.0

# Phase 2: Strategy validation  
max_quantity=5.0, max_notional=50.0

# Phase 3: Production simulation
max_quantity=10.0, max_notional=500.0

# Phase 4: Full paper trading
max_quantity=100.0, max_notional=5000.0
```

## How to Verify Current Limits

### **Method 1: Check the Code**
```bash
# View the current limits
grep -n "quantity >" app/trading/providers/alpaca.py
grep -n "notional >" app/trading/providers/alpaca.py
```

### **Method 2: Test with Excessive Values**
```python
import asyncio
from app.trading.providers import AlpacaProvider
from app.db.repository import AlphaRepository

async def test_limits():
    repo = AlphaRepository("data/alpha.db")
    provider = AlpacaProvider("key", "secret", repo, paper=True)
    
    # Test quantity limit
    try:
        result = await provider.submit_order(
            ticker="SPY",
            quantity=10.0,  # Exceeds limit
            direction="long"
        )
        print(f"Quantity test result: {result}")
    except Exception as e:
        print(f"Quantity test error: {e}")
    
    # Test notional limit
    try:
        result = await provider.submit_order(
            ticker="SPY", 
            notional=100.0,  # Exceeds limit
            direction="long"
        )
        print(f"Notional test result: {result}")
    except Exception as e:
        print(f"Notional test error: {e}")

asyncio.run(test_limits())
```

### **Method 3: Check Logs**
```bash
# Run a test and check for limit warnings
python test_limits.py
tail -f logs/paper_trading.log | grep "exceeds test limit"
```

## Summary

### **Current Limits**
- **Quantity**: 1 share maximum (line 53-55)
- **Notional**: $10 maximum (line 57-59)
- **Behavior**: Automatic capping with warning log

### **Control Options**
1. **Hard-coded change**: Edit the values directly
2. **Configurable**: Add parameters to constructor
3. **Environment-based**: Use environment variables

### **Recommendation**
Use **Option 2 (Configurable)** for flexibility and maintainability. This allows you to:
- Set different limits for different environments
- Change limits without code deployment
- Test different limit values easily
- Maintain backward compatibility

The limits are there for safety, but can be adjusted as your testing needs evolve.
