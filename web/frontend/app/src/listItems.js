import React from 'react';
import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import ListSubheader from '@material-ui/core/ListSubheader';
import BarChartIcon from '@material-ui/icons/BarChart';
import PlayCircleFilledIcon from '@material-ui/icons/PlayCircleFilled';
import List from '@material-ui/core/List';

export const History = function(props) {
  return (
  <div>
    <ListItem button>
      <ListItemIcon>
        <BarChartIcon />
      </ListItemIcon>
      <ListItemText primary="History" onClick={() => {props.history.push(`/dashboard`)}}/>
    </ListItem>
  </div>
  );
};



const algorithms = ['rbsa', 'ceus', 'mix'];

export const Pipelines = function(props) {
  function onClick (key) {
    props.history.push(`/dashboard/pipeline/${key}`);
  }

  return (
    <List>
    <div>
    <ListSubheader inset>Execute Algorithms</ListSubheader>
    {algorithms.map((item) =>
      <ListItem button key={item} onClick={() => {onClick(item)}}>
      <ListItemIcon>
        <PlayCircleFilledIcon />
      </ListItemIcon>
      <ListItemText primary={item} />
    </ListItem>)}
  </div>
    </List>
      );
  };

