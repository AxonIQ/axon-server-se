import React from 'react';
import { default as MUiTable } from '@material-ui/core/Table';
import { Paper } from '@material-ui/core';

type TableProps = {
  flat?: boolean;
  children: React.ReactNode;
};
export const Table = (props: TableProps) => (
  <MUiTable component={Paper} elevation={props.flat ? 0 : undefined}>
    {props.children}
  </MUiTable>
);
