def calculate_percentile(value, total):

    if value < 0 or total <= 0:
        raise ValueError("Both value and total must be non-negative and total must be greater than 0")
    
    percentile = (value / total) * 100

    print(f"The value {value} is at the {percentile:.2f}% percentile relative to a total of {total}.")
    return percentile

calculate_percentile(21, 1274)

# The value 21 is at the 1.65% percentile relative to a total of 1274.