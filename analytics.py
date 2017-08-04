import sys
from DB import DB

def main(argv):
    database = DB(argv[0], argv[1])



if __name__ == "__main__":
    main(sys.argv[1:])