import React from 'react';
import { createStore, applyMiddleware } from 'redux';
import { Provider } from 'react-redux';
import thunkMiddleware from 'redux-thunk';

import reducer from './model.js';
import KafkaStatus from './KafkaStatus.jsx';

const createStoreWithMiddleware = applyMiddleware(thunkMiddleware)(createStore);
const store = createStoreWithMiddleware(reducer);

const MountableApp = class StatusPageApp extends React.Component {
    render() {
        return <Provider store={store}><KafkaStatus dispatch={store.dispatch}/></Provider>
    }
}

export default MountableApp;
