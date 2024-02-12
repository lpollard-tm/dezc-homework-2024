## Workshop 1 Homework - dlt Workshop 

## Questions

### Question 1

What is the sum of the outputs of the generator for limit = 5?

* 10.234
* 7.892
* __*8.382*__
* 9.123

**ANSWER: 8.382**

```python
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

limit = 5
generator = square_root_generator(limit)
total = 0

for sqrt_value in generator:
    total += sqrt_value
    print(sqrt_value)

print(f"\nThe total sum of limit {limit} is: {total:.3f}")
```

```
1.0
1.4142135623730951
1.7320508075688772
2.0
2.23606797749979

The total sum of limit 5 is: 8.382
```

------------------------------------

### Question 2

What is the 13th number yielded by the generator?

* 4.236
* __*3.605*__
* 2.345
* 5.678

**ANSWER: 3.605**

```python
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

limit = 13
generator = square_root_generator(limit)
last_value = 0 

for sqrt_value in generator:
        last_value = sqrt_value
        print(sqrt_value)

print(f"\nThe {limit}th number yeilded  is: {last_value:.3f}")
```

```
1.0
1.4142135623730951
1.7320508075688772
2.0
2.23606797749979
2.449489742783178
2.6457513110645907
2.8284271247461903
3.0
3.1622776601683795
3.3166247903554
3.4641016151377544
3.605551275463989

The 13th number yeilded  is: 3.606
```

------------------------------------

### Question 3

Append the 2 generators. After correctly appending the data, calculate the sum of all ages of people.

* __*353*__
* 365
* 378
* 390

**ANSWER: 353**

------------------------------------

### Question 4

Merge the 2 generators using the ID column. Calculate the sum of ages of all the people loaded as described above.

* 215
* __*266*__
* 241
* 258

**ANSWER: 266**

------------------------------------

