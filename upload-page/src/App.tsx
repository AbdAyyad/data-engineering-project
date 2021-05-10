import React, {useState} from 'react';
import './App.css';
import './App.scss';
import {useCallback} from 'react'
import {useDropzone} from 'react-dropzone'
import {Button, Col, Container, Modal, Row} from 'react-bootstrap';
import axios from 'axios';

function App() {
    const [state, setState] = useState({show: false})
    const handleClose = () => {
        setState({show: false});
    }
    const onDrop = useCallback(acceptedFiles => {
        const formData = new FormData();
        // @ts-ignore
        formData.append('file', acceptedFiles[0]);
        axios.post(`${process.env.REACT_APP_SERVER_URL}/upload`, formData, {headers: {"Content-Type": "multipart/form-data"}})
            .then(result => {
                console.log('akal');
                setState({show: true});
            });
    }, [])
    const {getRootProps, getInputProps, isDragActive} = useDropzone({onDrop})

    return (
        <>
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
            <Modal
                show={state.show}
                onHide={handleClose}
                backdrop="static"
                keyboard={false}
                animation={false}
            >
                <Modal.Header closeButton>
                    <Modal.Title>upload completed</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    File uploaded to the server successfully.
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" onClick={handleClose}>
                        Close
                    </Button>
                </Modal.Footer>
            </Modal>
        </>
    )
}

export default App;