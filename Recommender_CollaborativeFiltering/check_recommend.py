import re
import operator
import sys


def mark(answer_path, output_path):
    answer_pairs = []
    output_pairs = []
    rvs_output_pairs = []

    with open(answer_path, 'r') as answer:
        for line in answer:
            raw_params = re.split("\s|,", line)
            params = [p for p in raw_params if p]   # remove empty string
            assert len(params) == 3, "Required 3 numbers per line, got {}".format(len(params))
            user_id = int(params[0])
            item_id = int(params[1])
            score = float(params[2])
            answer_pairs.append([user_id, item_id, score])

    with open(output_path, 'r') as answer:
        for line in answer:
            raw_params = re.split("\s|,", line)
            params = [p for p in raw_params if p]   # remove empty string
            assert len(params) == 3, "Required 3 numbers per line, got {}".format(len(params))
            user_id = int(params[0])
            item_id = int(params[1])
            score = float(params[2])
            output_pairs.append([user_id, item_id, score])
            rvs_output_pairs.append([user_id, item_id, score])

    answer_pairs = list(sorted(answer_pairs, key=operator.itemgetter(0, 1, 2)))
    output_pairs = list(sorted(output_pairs, key=operator.itemgetter(0, 1, 2)))
    if answer_pairs == output_pairs:
        return 2
    elif answer_pairs == rvs_output_pairs:
        return 1
    else:
        return 0


if __name__ == '__main__':
    result = mark(sys.argv[1], sys.argv[2])
    if result == 2:
        print("Test Passed")
    elif result == 1:
        print("Please reverse item_id and user_id")
    elif result == 0:
        print("Wrong Answer")
    else:
        assert False
