import React from 'react';
import { default as MUiTableHead } from '@material-ui/core/TableHead';
import { default as MUiTableRow } from '@material-ui/core/TableRow';
import './table-head.scss';

type TableHeadProps = {
  children: React.ReactNode;
};
export const TableHead = (props: TableHeadProps) => (
  <MUiTableHead classes={{ root: 'table-head' }}>
    <MUiTableRow>{props.children}</MUiTableRow>
  </MUiTableHead>
);
