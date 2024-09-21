import string
import requests
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import matplotlib.pyplot as plt
import logging

# Налаштування логування
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Функція для завантаження тексту з URL
def get_text(url: str) -> str:
    """
    Завантажує текст за заданою URL-адресою.
    
    :param url: URL для завантаження тексту.
    :return: Текст у вигляді рядка або None у разі помилки.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Перевірка на помилки HTTP
        return response.text
    except requests.RequestException as e:
        logging.error(f"Помилка при завантаженні тексту: {e}")
        return None

# Функція для видалення знаків пунктуації
def remove_punctuation(text: str) -> str:
    """
    Видаляє всі знаки пунктуації з тексту.
    
    :param text: Вхідний текст.
    :return: Текст без пунктуації.
    """
    return text.translate(str.maketrans("", "", string.punctuation))

# Map функція
def map_function(word: str) -> tuple[str, int]:
    """
    Повертає кортеж зі словом і числом 1 для мапінгу.
    
    :param word: Вхідне слово.
    :return: Кортеж (слово, 1).
    """
    return word, 1

# Shuffle функція
def shuffle_function(mapped_values: list[tuple[str, int]]) -> dict[str, list[int]]:
    """
    Перегруповує результати мапінгу, збираючи однакові слова разом.
    
    :param mapped_values: Список кортежів (слово, 1).
    :return: Перегруповані значення (слово, [1, 1, ...]).
    """
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

# Reduce функція
def reduce_function(key_values: tuple[str, list[int]]) -> tuple[str, int]:
    """
    Підраховує загальну кількість кожного слова.
    
    :param key_values: Кортеж (слово, список [1, 1, ...]).
    :return: Кортеж (слово, сума кількості).
    """
    key, values = key_values
    return key, sum(values)

# MapReduce функція
def map_reduce(text: str) -> dict[str, int]:
    """
    Виконує аналіз частоти слів у тексті за допомогою парадигми MapReduce.
    
    :param text: Вхідний текст для обробки.
    :return: Словник частот слів.
    """
    
    text = remove_punctuation(text)
    words = text.split()

    # Фільтрація слів довжиною 5 і більше символів
    words = [word for word in words if len(word) >= 4]

    # Паралельний Мапінг
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # Shuffle
    shuffled_values = shuffle_function(mapped_values)

    # Паралельна Редукція
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)

    

# Функція для візуалізації топ-слів
def visualize_top_words(word_freq: dict[str, int], top_n: int ) -> None:
    """
    Виводить кругову діаграму частоти топ N слів.
    
    :param word_freq: Словник з частотами слів.
    :param top_n: Кількість слів для відображення.
    """
    
    # Сортування та відбір топ слів
    sorted_words = sorted(word_freq.items(), key=lambda item: item[1], reverse=True)[:top_n]

    # Підготовка даних для візуалізації
    words, frequencies = zip(*sorted_words)
    colors = plt.cm.get_cmap('tab20', len(words)).colors  # Кольори для кругової діаграми

    # Виведення результатів у консоль
    print(f"Топ {top_n} слів за частотою використання:")
    for word, freq in sorted_words:
        print(f"{word}: {freq}")

    # Візуалізація
    plt.figure(figsize=(10, 8))  
    plt.pie(frequencies, labels=words, autopct='%1.1f%%', startangle=140, 
            colors=colors, textprops={'fontsize': 12, 'fontweight': 'bold'}) 
    plt.title(f'Top {top_n} Слів за Частотою Використання', fontsize=16, fontweight='bold', pad=40) 
    plt.axis('equal')  
    plt.tight_layout()  
    plt.show()
    



if __name__ == '__main__':
    # Вхідний текст для обробки
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    try:
        text = get_text(url)
        if text:
            # Виконання MapReduce на вхідному тексті
            result = map_reduce(text)

            # Виведення та візуалізація топ слів
            visualize_top_words(result, top_n=15)
        else:
            raise ValueError("Текст не був завантажений.")
    except Exception as e:
        logging.error(f"Помилка в головному блоці: {e}")
        
