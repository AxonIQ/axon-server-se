import classnames from "classnames";
import React from "react";
import "./link.scss";

type LinkProps = {
  to: string;
  type?: "primary" | "secondary" | "button";
  target?: string;
  fullWidth?: boolean;
  fullHeight?: boolean;
  underline?: boolean;
  active?: boolean;
  children: string | React.ReactNode;
};
export const Link = (props: LinkProps) => (
  <a
    href={props.to}
    className={classnames(
      "link",
      props.type && `link--${props.type}`,
      props.active && "link--active",
      props.underline && "link--underline",
      (props.fullWidth || props.fullHeight) && "link--inline-block",
      props.fullWidth && "link--full-width",
      props.fullHeight && "link--full-height"
    )}
    target={props.target}
  >
    {props.children}
  </a>
);
