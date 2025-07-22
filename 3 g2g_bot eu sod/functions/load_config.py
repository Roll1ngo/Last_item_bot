import yaml


def get_config():
    """Завантажує конфігурацію з YAML файлу."""
    try:
        with open('config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        print(f"Помилка: Файл {config.yaml} не знайдено.")
        return None
    except yaml.YAMLError as e:
        print(f"Помилка YAML: {e}")
        return None


if __name__ == '__main__':
    print(get_config())

