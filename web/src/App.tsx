import { Layout } from 'antd';
import { Logo } from './components/Logo';
import './App.css';
import { Sidebar } from './components/sidebar/Sidebar';
import { useRoutes } from 'react-router-dom';
import { router } from './router';

function App() {
    const routerElement = useRoutes(router);
    return (
        <Layout>
            <Layout.Header style={{backgroundColor: 'white'}}>
                <Logo></Logo>
            </Layout.Header>
            <Layout hasSider style={{height: 'calc(100vh - 64px)'}}>
                <Layout.Sider
                    theme='light'
                >
                    <Sidebar/>
                </Layout.Sider>
                <Layout.Content style={{padding: '20px'}}>
                    {
                        routerElement
                    }
                </Layout.Content>
            </Layout>
        </Layout>
    );
}

export default App;
