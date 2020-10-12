import { createMuiTheme } from "@material-ui/core";
import './theme.scss';

export const theme = createMuiTheme({
  typography: {
    fontFamily: ["Lato", "monospace"].join(","),
  },
});
