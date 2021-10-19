import React from 'react';
import { BrowserRouter, Route, Switch } from 'react-router-dom';
import Header from './components/layout/Header';
import ContentPanel from './components/layout/ContentPanel';
import Footer from './components/layout/Footer';
import Dashboard from './pages/Dashboard';
import PodPage from './pages/PodPage';
import AcknowledgementsPage from './pages/Acknowledgements';
import './App.css';

function App(): JSX.Element {
  return (
    <BrowserRouter>
      <div className="flex flex-col h-screen">
        <header className="flex">
          <Header />
        </header>
        <main className="flex flex-col flex-1">
          <ContentPanel>
            <Switch>
              <Route exact path={['/', '/pods']} component={Dashboard} />
              <Route exact path="/pods/:podName" component={PodPage} />
              <Route exact path="/acknowledgements" component={AcknowledgementsPage} />
            </Switch>
          </ContentPanel>
        </main>
        <footer className="flex">
          <Footer />
        </footer>
        <div id="portal" />
      </div>
    </BrowserRouter>
  );
}

export default App;
