import React from 'react';
import ReactDOM from 'react-dom';
import { Redirect, BrowserRouter, Route, Switch } from 'react-router-dom';
import Home from './home';
import SignIn from "./signin";
import SignUp from "./signup";
import ActivateEmail from "./activateEmail";
import ResultDirs from './result_dir';
import BaseDashboard from "./baseDashboard"
import Executions from "./executions";
import ExecutePipeline from "./executePipeline";
import ImageGallery from "./imageGallery"

const PrivateRoute = ({component: Component, ...rest}) => (
  <Route
    {...rest}
    render={props =>
      localStorage.getItem('auth_token')
        ? <Component {...props}/>
        : <Redirect to={{pathname: '/signin', state: {from: props.location}}}/>
    }
  />
);

const PublicRoute = ({component: Component, ...rest}) => (
  <Route
    {...rest}
    render={props =>
      localStorage.getItem('auth_token')
        ? <Redirect to={{pathname: '/dashboard', state: {from: props.location}}}/>
        : <Component {...props}/>
    }
  />
);

const dashboard = (Component) => (props) => (<BaseDashboard {...props}><Component {...props}/></BaseDashboard>);

function App() {
  return (
    <BrowserRouter>
      <Switch>
        <PublicRoute exact path="/" component={Home} />
        <PublicRoute path="/signin" component={SignIn} />
        <PublicRoute path="/signup" component={SignUp} />
        <PublicRoute path="/activate/:uid/:token" component={ActivateEmail} />
        <PrivateRoute path="/dashboard/pipeline/:pipeline_name" component={dashboard(ExecutePipeline)} />
        <PrivateRoute path="/dashboard/executions/:execution_id/results/:result_dir" component={dashboard(ImageGallery)} />
        <PrivateRoute path="/dashboard/executions/:execution_id" component={dashboard(ResultDirs)} />
        <PrivateRoute path="/dashboard" component={dashboard(Executions)}/>
      </Switch>
    </BrowserRouter>
  );
}

ReactDOM.render(<App />, document.getElementById('root'));

