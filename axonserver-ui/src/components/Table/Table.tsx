import React from 'react';
import { default as MUiTable } from '@material-ui/core/Table';

type TableProps = {
  children: React.ReactNode;
};
export const Table = (props: TableProps) => (
  <MUiTable>{props.children}</MUiTable>
);
