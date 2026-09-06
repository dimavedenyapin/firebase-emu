import React from 'react';
import ReactDOM from 'react-dom';
import App from './App';
import { createClient } from './firebase';
import { runProbe } from './probe';
import './style.css';
const client = createClient();
window.runFirebaseProbe = () => runProbe(client);
ReactDOM.render(<App client={client} />, document.getElementById('root'));
