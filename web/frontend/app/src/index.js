import React from 'react';
import ReactDOM from 'react-dom';
import { Redirect, BrowserRouter, Route, Switch } from 'react-router-dom';
import Home from './home';
import SignIn from "./signin";
import SignUp from "./signup";
import Dashboard from "./dashboard";


const PrivateRoute = ({component: Component, ...rest}) => (
  <Route
    {...rest}
    render={props =>
      localStorage.getItem('jwt_token')
        ? <Component {...props}/>
        : <Redirect to={{pathname: '/signin', state: {from: props.location}}}/>
    }
  />
);

const PublicRoute = ({component: Component, ...rest}) => (
  <Route
    {...rest}
    render={props =>
      localStorage.getItem('jwt_token')
        ? <Redirect to={{pathname: '/dashboard', state: {from: props.location}}}/>
        : <Component {...props}/>
    }
  />
);


function App() {
  return (
    <BrowserRouter>
      <Switch>
        <PublicRoute exact path="/" component={Home} />
        <PublicRoute path="/signin" component={SignIn} />
        <PublicRoute path="/signup" component={SignUp} />
        <PrivateRoute path="/dashboard" component={Dashboard} />
      </Switch>
    </BrowserRouter>
  );
}

ReactDOM.render(<App />, document.getElementById('root'));

