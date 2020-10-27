import { default as MUiDialogTitle } from '@material-ui/core/DialogTitle';
import IconButton from '@material-ui/core/IconButton';
import CloseIcon from '@material-ui/icons/Close';
import React from 'react';
import { Typography } from '../Typography/Typography';
import './dialog-title.scss';

type DialogTitleProps = {
  children: React.ReactNode;
  id?: string;
  onClose?: () => void;
};
export const DialogTitle = (props: DialogTitleProps) => (
  <MUiDialogTitle id={props.id} disableTypography>
    <div className="dialog-title">
      <Typography size="xl" weight="bold">
        {props.children}
      </Typography>
      {props.onClose ? (
        <IconButton
          className="dialog-title__close-button"
          aria-label="close"
          onClick={props.onClose}
        >
          <CloseIcon />
        </IconButton>
      ) : null}
    </div>
  </MUiDialogTitle>
);
