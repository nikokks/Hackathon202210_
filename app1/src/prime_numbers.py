from rustmath import compute_primes, compute_primes_sum

"""
Command: prime_numbers
Return a list of all the prime numbers inferior or equal to n
"""
def prime_numbers(n):
    if n <= 0:
        return "undefined"
    else:
        return compute_primes(n)


"""
Command: sum_prime_numbers
Return a sum of all the prime numbers inferior or equal to n
"""
def prime_numbers(n):
    if n <= 0:
        return "undefined"
    else:
        return compute_primes_sum(n)


