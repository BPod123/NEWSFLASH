from update_database import updateAllHeadlines
if __name__ == '__main__':
    # Get file path to webdriver
    f = open("../webdriverpath.txt", "r")
    wdpath = f.read()
    f.close()
    del f
    updateAllHeadlines(wdpath)