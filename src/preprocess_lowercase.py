import common


def make_lowercase(content_in):
    content_out = content_in.lower()
    return content_out


if __name__ == "__main__":
    print("begin making all text lowercase.")
    common.main(make_lowercase)
    print("done")

