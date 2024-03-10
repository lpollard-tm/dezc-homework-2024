def calculate_percentile(value, total):

    if value < 0 or total <= 0:
        raise ValueError("Both value and total must be non-negative and total must be greater than 0")
    
    percentile = (value / total) * 100

    print(f"The value {value} is at the {percentile:.2f}% percentile relative to a total of {total}.")
    return percentile

calculate_percentile(20, 1281)

# The value 20 is at the 1.56% percentile relative to a total of 1281.