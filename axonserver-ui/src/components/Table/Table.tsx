import { Paper } from '@material-ui/core';
import { default as MUiTable } from '@material-ui/core/Table';
import { default as MUiTableContainer } from '@material-ui/core/TableContainer';
import classnames from 'classnames';
import React from 'react';
import './table.scss';

type TableProps = {
  children: React.ReactNode;
  flat?: boolean;
  fixed?: boolean;
};
export const Table = (props: TableProps) => (
  <MUiTableContainer component={Paper} elevation={props.flat ? 0 : undefined}>
    <MUiTable className={classnames(props.fixed && 'table--fixed')}>
      {props.children}
    </MUiTable>
  </MUiTableContainer>
);
