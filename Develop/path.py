import os


def path_system():
    the_path = os.path.dirname(os.path.abspath(__file__))
    return the_path


if __name__ == '__main__':
    print(path_system())
