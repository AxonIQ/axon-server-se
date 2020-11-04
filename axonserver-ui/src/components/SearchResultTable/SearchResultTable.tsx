import React from 'react';
import { Table } from '../Table/Table';
import { TableBody } from '../TableBody/TableBody';
import { TableCell } from '../TableCell/TableCell';
import { TableHead } from '../TableHead/TableHead';
import { TableRow } from '../TableRow/TableRow';
import { Typography } from '../Typography/Typography';

type SearchResultTableProps = {
  headers: string[];
  data: Array<{
    idValues: number[];
    sortValues: number[];
    value: {
      [key: string]: any;
    };
  }>;
};
export const SearchResultTable = (props: SearchResultTableProps) => (
  <Table fixed flat>
    <TableHead>
      {props.headers.map((header, index) => (
        <TableCell key={`search-result-header-${index}`}>
          <Typography size="m" color="white" weight="bold">
            {header}
          </Typography>
        </TableCell>
      ))}
    </TableHead>

    <TableBody>
      {props.data.map((item, index) => (
        <TableRow
          onClick={() => console.log('hi!')}
          key={`search-result-data-row-${index}`}
        >
          {props.headers.map((header, itemIndex) => (
            <TableCell
              key={`search-result-data-item-${itemIndex}`}
              overflowElipsis
            >
              <Typography noBreak inline size="m" color="light">
                {item.value[header]}
              </Typography>
            </TableCell>
          ))}
        </TableRow>
      ))}
    </TableBody>
  </Table>
);
