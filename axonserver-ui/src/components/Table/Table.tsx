import { Paper } from '@material-ui/core';
import { default as MUiTable } from '@material-ui/core/Table';
import { default as MUiTableContainer } from '@material-ui/core/TableContainer';
import classnames from 'classnames';
import React from 'react';
import './table.scss';

type TableProps = {
  children: React.ReactNode;
  flat?: boolean;
  fixedLayout?: boolean;
  square?: boolean;
  stickyHeader?: boolean;
};
export const Table = (props: TableProps) => (
  <MUiTableContainer
    component={Paper}
    elevation={props.flat ? 0 : undefined}
    square={props.square}
  >
    <MUiTable
      stickyHeader={props.stickyHeader}
      className={classnames(props.fixedLayout && 'table--fixed')}
    >
      {props.children}
    </MUiTable>
  </MUiTableContainer>
);
