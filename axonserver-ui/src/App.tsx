import Container from '@material-ui/core/Container';
import Grid from '@material-ui/core/Grid';
import React from 'react';
import { BrowserRouter as Router, Switch, Route } from 'react-router-dom';
import { Navigation } from './components/Navigation/Navigation';
import { Settings } from './views/Settings/Settings';

const App = () => (
  <Router>
    <Container maxWidth={false} disableGutters={true}>
      <Grid container>
        <Grid item md={1}>
          <Navigation />
        </Grid>
        <Grid item md={11}>
          {/* A <Switch> looks through its children <Route>s and
                renders the first one that matches the current URL. */}
          <Switch>
            <Route path="/settings">
              <Settings />
            </Route>
          </Switch>
        </Grid>
      </Grid>
    </Container>
  </Router>
);

export default App;
