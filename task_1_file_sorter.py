import asyncio
from aiopath import AsyncPath
from aioshutil import copyfile
import argparse
import logging
from time import time
import datetime
import os


def parse_args():
    """
    Обробляє аргументи командного рядка для отримання шляхів до вихідної та цільової папок.
    
    Повертає:
    Namespace: Об'єкт, що містить аргументи source_folder і output_folder.
    """
    parser = argparse.ArgumentParser(description='Асинхронне сортування файлів за розширенням.')
    parser.add_argument('source_folder', type=str, help='Шлях до вихідної папки.')
    parser.add_argument('--output_folder', type=str, help='Шлях до папки призначення (необов’язково).')
    return parser.parse_args()


def setup_logging():
    """
    Налаштовує логування: створює файл для запису логів.
    """
    logging.basicConfig(
        filename='file_sorting.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )


def create_default_output_folder() -> str:
    """
    Створює директорію за замовчуванням з поточною датою і часом в тій же папці, де знаходиться файл скрипта.
    
    Повертає:
    str: Шлях до новоствореної папки.
    """
    current_directory = os.path.dirname(os.path.abspath(__file__))  # Шлях до папки з файлом скрипта
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    default_folder_name = f"sorted_files_{timestamp}"
    output_folder = os.path.join(current_directory, default_folder_name)
    return output_folder


async def read_folder(path: AsyncPath, output_folder: AsyncPath) -> None:
    """
    Рекурсивно читає файли у вихідній папці та копіює їх у відповідну підпапку за розширенням.
    
    Аргументи:
    path (AsyncPath): Шлях до вихідної папки.
    output_folder (AsyncPath): Шлях до папки призначення.
    """
    async for el in path.iterdir():
        if await el.is_dir():
            await read_folder(el, output_folder)
        else:
            try:
                await copy_file(el, output_folder)
            except Exception as e:
                logging.error(f"Помилка під час копіювання файлу {el}: {e}")
                print(f"Помилка під час копіювання файлу {el}. Деталі у лог-файлі.")


async def copy_file(file: AsyncPath, output_folder: AsyncPath) -> None:
    """
    Копіює файл у підпапку за його розширенням.
    
    Аргументи:
    file (AsyncPath): Шлях до файлу.
    output_folder (AsyncPath): Шлях до папки призначення.
    """
    ext = file.suffix.lower().strip('.')
    new_path = output_folder / ext

    try:
        # Створюємо папку, якщо її не існує
        await new_path.mkdir(exist_ok=True, parents=True)
        logging.info(f"Створено папку для розширення .{ext}: {new_path}")
    except Exception as e:
        logging.error(f"Помилка створення папки {new_path}: {e}")
        raise

    try:
        print(f"Копіюється файл {file.name} до {new_path}")
        logging.info(f"Копіюється файл {file.name} до {new_path}")
        await copyfile(file, new_path / file.name)
    except Exception as e:
        logging.error(f"Помилка копіювання файлу {file.name}: {e}")
        raise


async def main():
    args = parse_args()

    source = AsyncPath(args.source_folder)
    output_folder = AsyncPath(args.output_folder or create_default_output_folder())

    # Перевірка на існування вихідної папки
    if not await source.exists() or not await source.is_dir():
        print(f"Вихідна папка {source} не існує або це не директорія.")
        logging.error(f"Вихідна папка {source} не існує або це не директорія.")
        return

    await output_folder.mkdir(exist_ok=True, parents=True)
    

    print(f"Сортування файлів з папки: {source}")
    print(f"Файли будуть розміщені у папці: {output_folder}")
    logging.info(f"Початок сортування файлів з папки {source} до {output_folder}")

    start = time()
    
    task = asyncio.create_task(read_folder(source, output_folder))
    
    await task
   
    elapsed_time = time() - start
    logging.info(f"Завдання завершилося успішно за {elapsed_time:.2f} секунд.")
    print(f"Завдання завершилося успішно")
    print(f"Час виконання: {elapsed_time:.2f} секунд.")


if __name__ == '__main__':
    setup_logging()  # Налаштовуємо логування
    asyncio.run(main())
