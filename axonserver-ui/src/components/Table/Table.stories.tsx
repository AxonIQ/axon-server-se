import React from 'react';
import { TableHead } from '../TableHead/TableHead';
import { TableBody } from '../TableBody/TableBody';
import { TableCell } from '../TableCell/TableCell';
import { TableRow } from '../TableRow/TableRow';
import { Table } from './Table';
import { Typography } from '../Typography/Typography';

export default {
  title: 'Components/Table',
  component: Table,
};

export const Primary = () => (
  <Table>
    <TableHead>
      <TableCell smallWidth>
        <Typography size="m" uppercase weight="bold" color="white">
          cell 1
        </Typography>
      </TableCell>
      <TableCell>
        <Typography size="m" uppercase weight="bold" color="white">
          cell 2
        </Typography>
      </TableCell>
    </TableHead>
    <TableBody>
      <TableRow>
        <TableCell smallWidth>
          <Typography size="m">Hello from cell1!</Typography>
        </TableCell>
        <TableCell>
          <Typography size="m">Hello from cell2!</Typography>
        </TableCell>
      </TableRow>
    </TableBody>
  </Table>
);

export const Flat = () => (
  <Table flat>
    <TableHead>
      <TableCell smallWidth>
        <Typography size="m" uppercase weight="bold" color="white">
          cell 1
        </Typography>
      </TableCell>
      <TableCell>
        <Typography size="m" uppercase weight="bold" color="white">
          cell 2
        </Typography>
      </TableCell>
    </TableHead>
    <TableBody>
      <TableRow>
        <TableCell smallWidth>
          <Typography size="m">Hello from cell1!</Typography>
        </TableCell>
        <TableCell>
          <Typography size="m">Hello from cell2!</Typography>
        </TableCell>
      </TableRow>
    </TableBody>
  </Table>
);
