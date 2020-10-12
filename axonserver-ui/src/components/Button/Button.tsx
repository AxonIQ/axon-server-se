import { default as MUiButton } from "@material-ui/core/Button";
import classnames from "classnames";
import React from "react";
import "./button.scss";

type ButtonProps = {
  type?: "primary" | "secondary";
  onClick?: (event: React.MouseEvent) => void;
  children?: React.ReactNode;
};
export const Button = (props: ButtonProps) => (
  <MUiButton
    className={classnames("button", props.type && `button-${props.type}`)}
    onClick={props.onClick}
  >
    {props.children}
  </MUiButton>
);
