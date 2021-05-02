import React from 'react';
import './App.css';
import './App.scss';
import {useCallback} from 'react'
import {useDropzone} from 'react-dropzone'
import {Col, Container, Row} from 'react-bootstrap';


function App() {
    const onDrop = useCallback(acceptedFiles => {
        // Do something with the files
        console.log('akal');
    }, [])
    const {getRootProps, getInputProps, isDragActive} = useDropzone({onDrop})

    return (
        <main>
            <Container>
                <Row>
                    <Col md={'2'}/>
                    <Col md={'8'} className={'uploadZone'}>
                        <div {...getRootProps()} className={'text-area'}>
                            <input {...getInputProps()} />
                            {
                                isDragActive ?
                                    <p>Drop the files here ...</p> :
                                    <p>Drag 'n' drop some files here, or click to select files</p>
                            }
                        </div>
                    </Col>
                    <Col md={'2'}/>
                </Row>
            </Container>
        </main>
    )
}

export default App;
