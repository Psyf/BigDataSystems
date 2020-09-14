import operator
import sys


def mark(answer_path, output_path):
    answer_dict = {}
    output_dict = {}

    min_count = 0x7fffffff
    with open(answer_path, 'r') as answer:
        for line in answer:
            params = line.split()
            count = int(params[0])
            word = params[1]
            min_count = min(min_count, count)
            if count in answer_dict:
                answer_dict[count].add(word)
            else:
                answer_dict[count] = {word}

    count_list = []
    with open(output_path, 'r') as output:
        for line in output:
            params = line.split()
            count = int(params[0])
            word = params[1]
            count_list.append(count)
            if count in output_dict:
                output_dict[count].add(word)
            else:
                output_dict[count] = {word}

    # check correctness
    if count_list != list(sorted(count_list)[::-1]):
        return -1

    for count in answer_dict.keys():
        if count != min_count:
            if output_dict[count] != answer_dict[count]:
                return -2
            del output_dict[count]
        if count == min_count:
            if not output_dict[count].issubset(answer_dict[count]):
                return -3
            del output_dict[count]

    if output_dict:
        return -4
    else:
        return 1


if __name__ == '__main__':
    result = mark(sys.argv[1], sys.argv[2])
    if result == 1:
        print("Test Passed")
    elif result == -1:
        print("Wrong Answer: incorrect order")
    elif result == -4:
        print("Wrong Answer: output exceeds top 20")
    elif result <= 0:
        print("Wrong Answer")
