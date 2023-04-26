import re
import argparse

from enum import Enum
from pathlib import Path
from typing import NamedTuple

import polars as pl


MAX_NODES = 200_000
MAX_ELEMS = 1_000_000
TOLERANCE = 0.001

REGEX_STRICT_FLOAT_NUMBER = r"(?:[+-]?\d+)(?:\.\d+)(?:[eE][+-]?\d+)?"
REGEX_NODE = (
    r"(\d+)"
    r"\s+"
    f"({REGEX_STRICT_FLOAT_NUMBER})"
    r"\s+"
    f"({REGEX_STRICT_FLOAT_NUMBER})"
    r"\s+"
    f"({REGEX_STRICT_FLOAT_NUMBER})"
)

REGEX_TETRA_ELEMENT = r"(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)"

NODE_SCHEMA = [
    ("node", pl.Int32),
    ("x", pl.Float64),
    ("y", pl.Float64),
    ("z", pl.Float64),
]

ELEM_SCHEMA = [
    ("elem", pl.Int32),
    ("f1", pl.Int32),
    ("f2", pl.Int32),
    ("f3", pl.Int32),
    ("f4", pl.Int32),
    ("nf1", pl.Int32),
    ("nf2", pl.Int32),
    ("nf3", pl.Int32),
    ("nf4", pl.Int32),
]


class FileArgs(NamedTuple):
    in_file: str
    out_grid_file: str
    out_bc_file: str


class FilePaths(NamedTuple):
    in_file: str
    out_grid_file: str
    out_bc_file: str


class Node(Enum):
    sentinal = 0
    node = "number"
    x = "x"
    y = "y"
    z = "z"


class Elem(Enum):
    sentinal = 0
    elem = "number"
    f1 = "f1"
    f2 = "f2"
    f3 = "f3"
    f4 = "f4"
    nf1 = "nf1"
    nf2 = "nf2"
    nf3 = "nf3"
    nf4 = "nf4"


class Inflow(Enum):
    supersonic = 1
    subsonic = 2


class Symmetry(Enum):
    x = 41
    y = 42
    z = 43


class BCType(Enum):
    inflow = Inflow
    outflow = 3
    symmetry = Symmetry
    wall = 5


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("in_file", help="Input file path")
    parser.add_argument("out_grid_file", help="Output file path")
    parser.add_argument("out_bc_file", help="Output file path")

    args: FileArgs = parser.parse_args()
    files = FilePaths(
        Path(args.in_file), Path(args.out_grid_file), Path(args.out_bc_file)
    )

    return files


def node_dict(node: int, x: float = None, y: float = None, z: float = None):
    args = locals()
    if node == Node.sentinal:
        return {getattr(Node, arg).value: [] for arg in args.keys()}
    return {
        getattr(Node, arg).value: [Node.sentinal.value if value is None else value]
        for arg, value in args
    }


def elem_dict(
    elem: int,
    f1: int = None,
    f2: int = None,
    f3: int = None,
    f4: int = None,
    nf1: int = None,
    nf2: int = None,
    nf3: int = None,
    nf4: int = None,
):
    args = locals()
    if elem == Elem.sentinal:
        return {getattr(Node, arg).value: [] for arg in args.keys()}
    return {
        getattr(Node, arg).value: [Elem.sentinal.value if value is None else value]
        for arg, value in args
    }


def load_node_and_elems(files, df_nodes, df_elems):
    with open(files.in_file, mode="r") as i_file:
        for line in i_file:
            reg_res_node = re.search(REGEX_NODE, line)
            reg_res_elem = re.search(REGEX_TETRA_ELEMENT, line)
            if reg_res_node:
                df_node = pl.DataFrame(
                    node_dict(
                        int(reg_res_node.group(1)),
                        float(reg_res_node.group(2)),
                        float(reg_res_node.group(3)),
                        float(reg_res_node.group(4)),
                    ),
                    schema=NODE_SCHEMA,
                )
                pl.concat([df_nodes, df_node], how="vertical")
            if reg_res_elem:
                df_elem = pl.DataFrame(
                    elem_dict(
                        int(reg_res_node.group(1)),
                        int(reg_res_node.group(2)),
                        int(reg_res_node.group(3)),
                        int(reg_res_node.group(4)),
                        int(reg_res_node.group(5)),
                    ),
                    schema=ELEM_SCHEMA,
                )
                pl.concat([df_elems, df_elem], how="vertical")


def main():
    df_nodes = pl.DataFrame(node_dict(Node.sentinal), schema=NODE_SCHEMA)
    df_elems = pl.DataFrame(elem_dict(Elem.sentinal), schema=ELEM_SCHEMA)

    files = get_args()

    load_node_and_elems(files, df_nodes, df_elems)

    max_nodes = df_nodes[:, Node.node.value].max().item()
    max_elems = df_elems[:, Elem.node.value].max().item()

    x_max = df_nodes[:, Node.x.value].max().item()
    x_min = df_nodes[:, Node.x.value].min().item()

    num_of_ghost = max_elems

    with open(files.out_grid_file, mode="w") as out_grid_file, open(
        files.out_bc_file, mode="w"
    ) as out_bc_file:
        out_grid_file.write(f"{max_nodes} {max_elems}")
        for i, x, y, z in df_nodes[:, :]:
            out_grid_file.write(f"{i}  {x.item():.4f}  {y.item():.4f}  {z.item():.4f}")

        for i in range(max_elems):
            f1, f2, f3, f4 = [ser.item() for ser in df_elems[i, 1:5]]
            for j in range(max_elems):
                if i == j:
                    continue
                if Elem.sentinal.value == df_elems[i, Elem.nf1.value] and all(
                    any(f2 == ser.item() for ser in df_elems[i, 1:5]),
                    any(f3 == ser.item() for ser in df_elems[i, 1:5]),
                    any(f4 == ser.item() for ser in df_elems[i, 1:5]),
                ):
                    df_elems[i, Elem.nf1.value] = j
                if Elem.sentinal.value == df_elems[i, Elem.nf2.value] and all(
                    any(f1 == ser.item() for ser in df_elems[i, 1:5]),
                    any(f3 == ser.item() for ser in df_elems[i, 1:5]),
                    any(f4 == ser.item() for ser in df_elems[i, 1:5]),
                ):
                    df_elems[i, Elem.nf2.value] = j
                if Elem.sentinal.value == df_elems[i, Elem.nf3.value] and all(
                    any(f1 == ser.item() for ser in df_elems[i, 1:5]),
                    any(f2 == ser.item() for ser in df_elems[i, 1:5]),
                    any(f4 == ser.item() for ser in df_elems[i, 1:5]),
                ):
                    df_elems[i, Elem.nf3.value] = j
                if Elem.sentinal.value == df_elems[i, Elem.nf4.value] and all(
                    any(f1 == ser.item() for ser in df_elems[i, 1:5]),
                    any(f2 == ser.item() for ser in df_elems[i, 1:5]),
                    any(f3 == ser.item() for ser in df_elems[i, 1:5]),
                ):
                    df_elems[i, Elem.nf4.value] = j

            if Elem.sentinal.value == df_elems[i, Elem.nf1.value]:
                num_of_ghost += 1
                df_elems[i, Elem.nf1.value] = num_of_ghost

                x_cen = (
                    df_nodes[f2, Node.x.value].item()
                    + df_nodes[f3, Node.x.value].item()
                    + df_nodes[f4, Node.x.value].item()
                ) / 3.0

                if abs(x_cen - x_min) < TOLERANCE:
                    out_bc_file.write(f"{i} {num_of_ghost} {BCType.inflow.value.subsonic.value} 1")
                elif abs(x_cen - x_max) < TOLERANCE:
                    out_bc_file.write(f"{i} {num_of_ghost} {BCType.outflow.value} 1")
                else:
                    out_bc_file.write(f"{i} {num_of_ghost} {BCType.wall.value} 1")
            if Elem.sentinal.value == df_elems[i, Elem.nf2.value]:
                num_of_ghost += 1
                df_elems[i, Elem.nf2.value] = num_of_ghost

                x_cen = (
                    df_nodes[f1, Node.x.value].item()
                    + df_nodes[f3, Node.x.value].item()
                    + df_nodes[f4, Node.x.value].item()
                ) / 3.0

                if abs(x_cen - x_min) < TOLERANCE:
                    out_bc_file.write(f"{i} {num_of_ghost} {BCType.inflow.value.subsonic.value} 2")
                elif abs(x_cen - x_max) < TOLERANCE:
                    out_bc_file.write(f"{i} {num_of_ghost} {BCType.outflow.value} 2")
                else:
                    out_bc_file.write(f"{i} {num_of_ghost} {BCType.wall.value} 2")
            if Elem.sentinal.value == df_elems[i, Elem.nf3.value]:
                num_of_ghost += 1
                df_elems[i, Elem.nf3.value] = num_of_ghost

                x_cen = (
                    df_nodes[f1, Node.x.value].item()
                    + df_nodes[f2, Node.x.value].item()
                    + df_nodes[f4, Node.x.value].item()
                ) / 3.0

                if abs(x_cen - x_min) < TOLERANCE:
                    out_bc_file.write(f"{i} {num_of_ghost} {BCType.inflow.value.subsonic.value} 3")
                elif abs(x_cen - x_max) < TOLERANCE:
                    out_bc_file.write(f"{i} {num_of_ghost} {BCType.outflow.value} 3")
                else:
                    out_bc_file.write(f"{i} {num_of_ghost} {BCType.wall.value} 3")
            if Elem.sentinal.value == df_elems[i, Elem.nf4.value]:
                num_of_ghost += 1
                df_elems[i, Elem.nf4.value] = num_of_ghost

                x_cen = (
                    df_nodes[f1, Node.x.value].item()
                    + df_nodes[f2, Node.x.value].item()
                    + df_nodes[f3, Node.x.value].item()
                ) / 3.0

                if abs(x_cen - x_min) < TOLERANCE:
                    out_bc_file.write(f"{i} {num_of_ghost} {BCType.inflow.value.subsonic.value} 4")
                elif abs(x_cen - x_max) < TOLERANCE:
                    out_bc_file.write(f"{i} {num_of_ghost} {BCType.outflow.value} 4")
                else:
                    out_bc_file.write(f"{i} {num_of_ghost} {BCType.wall.value} 4")
            
            nf1, nf2, nf3, nf4 = [ser.item() for ser in df_elems[i, 5:9]]

            out_grid_file.write(f"{i} {f1} {f2} {f3} {f4} {nf1} {nf2} {nf3} {nf4}")
