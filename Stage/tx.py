from cryptography.fernet import Fernet

cipher = Fernet('lNXHXIz61VOA6Q1Zc1v5K-udwN1dEfHK8d8DBXA3-MQ=')
text = b'My super secret message'
encrypted_text = cipher.encrypt(b'123123123123')
t = encrypted_text.decode('utf-8')
print(encrypted_text)
decrypted_text = cipher.decrypt(encrypted_text)
print(decrypted_text.decode("utf-8"))