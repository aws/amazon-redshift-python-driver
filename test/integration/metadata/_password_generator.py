import secrets
import string


def generate_password(length: int = 16) -> str:
    """
    Generates a secure password with specified length containing lowercase, uppercase letters and digits.

    Args:
        length (int): The desired length of the password. Defaults to 16 characters.

    Returns:
        str: A generated password string
    """

    # Security check: Ensure minimum password length
    if length < 12:
        raise ValueError("Password length must be at least 12 characters")

    # Define character sets using string module
    lowercase: str = string.ascii_lowercase
    uppercase: str = string.ascii_uppercase
    digits: str = string.digits
    all_chars = lowercase + uppercase + digits

    # Ensure we have at least one of each required type
    password = [
        secrets.choice(lowercase),
        secrets.choice(uppercase),
        secrets.choice(digits)
    ]

    #  Fill the rest with random characters from all categories
    for _ in range(length - 3):
        password.append(secrets.choice(all_chars))

    # Shuffle the password characters
    secrets.SystemRandom().shuffle(password)

    return ''.join(password)
