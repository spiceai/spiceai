import React from 'react';
import { BrowserRouter, Route, Switch } from 'react-router-dom';
import Header from './components/layout/Header';
import ContentPanel from './components/layout/ContentPanel';
import Footer from './components/layout/Footer';
import Dashboard from './pages/Dashboard';
import PodPage from './pages/PodPage';
import './App.css';

function App(): JSX.Element {
  return (
    <BrowserRouter>
      <div className="flex flex-col flex-grow h-screen">
        <Header />
        <main className="flex flex-col flex-grow">
          <ContentPanel>
            <Switch>
              <Route exact path={['/', '/pods']} component={Dashboard} />
              <Route exact path="/pods/:podName" component={PodPage} />
            </Switch>
          </ContentPanel>
        </main>
        <Footer />
      </div>
    </BrowserRouter>
  );
}

export default App;
