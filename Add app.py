def calculate_apr(principal, rate, days, compound=False):
    daily_rate = rate / 100 / 365

    if compound:
        return principal * ((1 + daily_rate) ** days)
    else:
        return principal * (1 + daily_rate * days)
