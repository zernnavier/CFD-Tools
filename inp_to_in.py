import re
import argparse

from pathlib import Path
from typing import NamedTuple


CLEAR_SCREEN_STRING_CODE = "\033c"
SCRIPT_PRECISION = 11
AVG_CHARS_LIMIT_FOR_DOTS = 96
MAX_CHARS_LIMIT_FOR_DOTS = 97
SIGN_DIGIT_DOT = 3
REGEX_FLOAT_NUMBER = r"([+-]?\d+)(\.\d+)?([eE][+-]?\d+)?"
REGEX_STRICT_FLOAT_NUMBER = r"(?:[+-]?\d+)(?:\.\d+)(?:[eE][+-]?\d+)?"
REGEX_SPACE_AROUND_COMMAS = r"\s*,\s*"
REGEX_NODE = (
    r"(\d+)"
    f"{REGEX_SPACE_AROUND_COMMAS}"
    f"({REGEX_STRICT_FLOAT_NUMBER})"
    f"{REGEX_SPACE_AROUND_COMMAS}"
    f"({REGEX_STRICT_FLOAT_NUMBER})"
    f"{REGEX_SPACE_AROUND_COMMAS}"
    f"({REGEX_STRICT_FLOAT_NUMBER})"
)
REGEX_TETRA_ELEMENT = r"(\d+),\s+(\d+),\s+(\d+),\s+(\d+),\s+(\d+)"


class FileArgs(NamedTuple):
    in_file: str
    out_file: str


class FilePaths(NamedTuple):
    in_file: str
    out_file: str


class NodeElemMax(NamedTuple):
    node: int
    elem: int
    total: int


class NodeElemJLength(NamedTuple):
    node: int
    elem: int


class NodeElem(NamedTuple):
    size: NodeElemMax
    length: NodeElemJLength


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("in_file", help="Input file path")
    parser.add_argument("out_file", help="Output file path")

    args: FileArgs = parser.parse_args()
    files = FilePaths(Path(args.in_file), Path(args.out_file))

    return files


def formatted_floating_point_value(
    decimal: float, precision: int, exp_digits: int
) -> str:
    """
    Generate fixed precision and fixed exponent digit floating point string in base of 10
    represented by 'e'

    @params decimal_value: float - value to be formatted
    @params precision: int - required precision in the output string
    @params exp_digits: int - number digits to present the exponent power
    @Return: str - float point string
    """
    float_str = f"{decimal:.{precision}e}"
    reg_res = re.search(REGEX_FLOAT_NUMBER, float_str)
    exp_char, exp_sign, *exp_num_array = reg_res.group(3)
    exp_num = abs(int("".join(exp_num_array)))
    float_value_str = reg_res.group(1) + reg_res.group(2)
    return (
        f"{float_value_str.rjust(precision+SIGN_DIGIT_DOT)}"
        f"{exp_char}{exp_sign}{exp_num:0{exp_digits}d}"
    )


def get_justify_size(read_file: Path) -> NodeElem:
    node_count, elem_count = 0, 0
    node, elem = "", ""
    with open(read_file, mode="r") as i_file:
        for line in i_file:
            print(line)
            reg_res_node = re.search(REGEX_NODE, line)
            reg_res_elem = re.search(REGEX_TETRA_ELEMENT, line)
            if reg_res_node:
                node_count += 1
                node = reg_res_node.group(1)
            if reg_res_elem:
                elem_count += 1
                elem = reg_res_elem.group(1)
    print(f"{node = }, {elem = }, {node_count = }, {elem_count = }")
    return NodeElem(
        NodeElemMax(int(node), int(elem), node_count + elem_count),
        NodeElemJLength(len(node), len(elem)),
    )


def print_download_bar_percentage(percentage: int):
    print(CLEAR_SCREEN_STRING_CODE, end="")
    if percentage <= AVG_CHARS_LIMIT_FOR_DOTS:
        print(
            f"{'.'*percentage}{' '*(AVG_CHARS_LIMIT_FOR_DOTS-percentage)}"
            f"{str(percentage).rjust(len('100'))}%"
        )
    elif MAX_CHARS_LIMIT_FOR_DOTS < percentage < 100:
        print(f"{'.'*MAX_CHARS_LIMIT_FOR_DOTS}{percentage}%")
    else:
        print(f"{'.'*AVG_CHARS_LIMIT_FOR_DOTS}100%")


def main():
    ffpv = lambda x: formatted_floating_point_value(float(x), SCRIPT_PRECISION, 3)

    files = get_args()

    with open(files.in_file, mode="r") as i_file, open(
        files.out_file, mode="w"
    ) as o_file:
        maximum, j_length = get_justify_size(files.in_file)
        max_made, _, total = maximum
        max_elem = total - max_made
        node_just, elem_just = j_length
        o_file.write(f"{max_made}    {max_elem}" + "\n")
        elem_count, node_count = 0, 0
        old_percentage, curr_percentage, processed_line = 0, 0, ""
        print_download_bar_percentage(curr_percentage)
        for line in i_file:
            reg_res_node = re.search(REGEX_NODE, line)
            reg_res_elem = re.search(REGEX_TETRA_ELEMENT, line)
            if reg_res_node:
                node_count += 1
                processed_line = (
                    f"{reg_res_node.group(1).rjust(node_just)}"
                    f" {ffpv(reg_res_node.group(2))}"
                    f" {ffpv(reg_res_node.group(3))}"
                    f" {ffpv(reg_res_node.group(4))}"
                )
            if reg_res_elem:
                elem_count += 1
                processed_line = (
                    f"{str(elem_count).rjust(elem_just)}"
                    "  6"
                    "  4 "
                    f"   {reg_res_elem.group(2).rjust(node_just)}"
                    f"   {reg_res_elem.group(3).rjust(node_just)}"
                    f"   {reg_res_elem.group(4).rjust(node_just)}"
                    f"   {reg_res_elem.group(5).rjust(node_just)}"
                )
            if reg_res_node or reg_res_elem:
                o_file.write(processed_line + "\n")
                curr_percentage = int((node_count + elem_count) / total * 100)
            if (curr_percentage - old_percentage) == 1:
                print_download_bar_percentage(curr_percentage)
                old_percentage = curr_percentage

    print(f"Grid saved to file '{files.out_file}'")

    # contents = await read_file('input.txt')
    # await write_file('output.txt', contents)


if __name__ == "__main__":
    main()
    pass
