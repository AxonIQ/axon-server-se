import React from "react";
import classnames from "classnames";
import { default as MUiTypography } from "@material-ui/core/Typography";
import "./typography.scss";
import useMediaQuery from "@material-ui/core/useMediaQuery";
import { useTheme } from "@material-ui/core/styles";

type TypographyProps = {
  inline?: boolean;
  size: "s" | "m" | "l" | "xl" | "xxl" | "xxxl";
  weight?: "lighter" | "light" | "bold";
  tag?: "h1" | "h2" | "h3" | "h4" | "h5" | "h6" | "div" | "span" | "p";
  color?: "dark" | "lighter" | "light" | "primary" | "secondary";
  noBreak?: boolean;
  italic?: boolean;
  center?: boolean;
  addMargin?: boolean;
  children: string | React.ReactNode;
};
export const Typography = (props: TypographyProps) => {
  const theme = useTheme();
  const matches = useMediaQuery(theme.breakpoints.down("sm"));

  return (
    <MUiTypography
      className={classnames(
        "typography",
        !props.noBreak && "typography--break",
        props.color && `typography-color--${props.color}`,
        props.weight && `typography-weight--${props.weight}`,
        props.italic && "typography--italic",
        props.center && "typography--center",
        props.inline && "typography--inline",
        props.addMargin && "typography--margin",
        `typography-size--${props.size}`,
        matches && `typography-size--${props.size}-sm-down`
      )}
      component={props.tag || "div"}
    >
      {props.children}
    </MUiTypography>
  );
};
