from io import BufferedReader
from unittest import mock

import pytest
from aiobotocore.response import StreamingBody
from botocore.exceptions import ClientError

from some_stuff.adapters.s3.repositories.exceptions import SomeStuffRepositoryError
from some_stuff.config import settings


@pytest.fixture
async def streaming_body_mock():
    return mock.MagicMock(spec=StreamingBody)


async def test_put_some_stuff_success(SomeStuff_repository, s3_response_mock):
    s3_client_mock = SomeStuff_repository.s3_client
    s3_client_mock.put_object = s3_response_mock

    data = mock.MagicMock(spec=BufferedReader)
    s3_client_mock = SomeStuff_repository.s3_client

    result = await SomeStuff_repository.put_some_stuff('some_stuff.png', data, 'image/png')
    assert result is True
    s3_client_mock.put_object.assert_called_once_with(
        Bucket=settings.S3_BUCKET,
        Key='some_stuff.png',
        Body=data,
        ContentType='image/png'
    )


@pytest.mark.parametrize(
    'response_dict, expected_result',
    (
            (
                {'ResponseMetadata': {'HTTPStatusCode': 403}},
                "Upload some_stuff object_name='some_stuff.png' failed. Response code: 403",
            ),
            (
                {},
                "Unexpected response for request_kwargs=",
            ),
    )
)
async def test_put_some_stuff_failes(SomeStuff_repository,
                               s3_response_mock,
                               response_dict,
                               expected_result):
    s3_client_mock = SomeStuff_repository.s3_client
    s3_client_mock.put_object = s3_response_mock

    data = mock.MagicMock(spec=BufferedReader)
    s3_client_mock = SomeStuff_repository.s3_client

    async def return_response():
        return response_dict

    s3_response_mock.return_value = return_response()

    with pytest.raises(SomeStuffRepositoryError) as excinfo:
        await SomeStuff_repository.put_some_stuff('some_stuff.png', data, 'image/png')
    assert expected_result in str(excinfo.value)
    s3_client_mock.put_object.assert_called_once_with(
        Bucket=settings.S3_BUCKET,
        Key='some_stuff.png',
        Body=data,
        ContentType='image/png'
    )


async def test_get_some_stuff_success(SomeStuff_repository, s3_response_mock, streaming_body_mock):
    object_name = 'some_stuff.png'
    s3_client_mock = SomeStuff_repository.s3_client
    s3_client_mock.get_object = s3_response_mock

    async def return_response():
        return {'Body': streaming_body_mock, 'ResponseMetadata': {'HTTPStatusCode': 200}}

    s3_response_mock.return_value = return_response()

    result = await SomeStuff_repository.get_some_stuff(object_name)

    assert result == streaming_body_mock
    s3_client_mock.get_object.assert_called_once_with(
        Bucket=SomeStuff_repository.bucket,
        Key=object_name
    )


@pytest.mark.parametrize(
    'response_data, expected_result',
    (
            (
                {'ResponseMetadata': {'HTTPStatusCode': 403}},
                "Get some_stuff object_name='some_stuff.png' failed. Response code: 403",
            ),
            (
                {},
                "Unexpected response for request_kwargs={'Key': 'some_stuff.png'}, command='get_object'",
            ),
    )
)
async def test_get_some_stuff_failes(SomeStuff_repository,
                               s3_response_mock,
                               response_data,
                               expected_result):
    object_name = 'some_stuff.png'
    s3_client_mock = SomeStuff_repository.s3_client
    s3_client_mock.get_object = s3_response_mock

    async def return_response():
        if isinstance(response_data, Exception):
            raise response_data
        return response_data

    s3_response_mock.return_value = return_response()

    with pytest.raises(SomeStuffRepositoryError) as excinfo:
        await SomeStuff_repository.get_some_stuff(object_name)
    assert expected_result in str(excinfo.value)
    s3_client_mock.get_object.assert_called_once_with(
        Bucket=SomeStuff_repository.bucket,
        Key=object_name
    )


async def test_get_some_stuff_no_such_key(SomeStuff_repository, s3_response_mock):
    object_name = 'some_stuff.png'
    s3_client_mock = SomeStuff_repository.s3_client
    s3_client_mock.get_object = s3_response_mock
    s3_response_mock.side_effect = ClientError(
        {
            'Error': {'Code': 'NoSuchKey', 'Message': 'The specified key does not exist.', 'Key': 'sUntitled 2.csv',
                      'BucketName': 'test1', 'Resource': '/test1/sUntitled 2.csv'},
            'ResponseMetadata': {'HTTPStatusCode': 404}
        },
        'GetObject'
    )
    with pytest.raises(SomeStuffRepositoryError) as excinfo:
        await SomeStuff_repository.get_some_stuff(object_name)
    expected_result = ("request_kwargs={'Key': 'some_stuff.png'}, command='get_object'. Error error={'Code': 'NoSuchKey', "
                       "'Message': 'The specified key does not exist.', 'Key': 'sUntitled 2.csv', 'BucketName': "
                       "'test1', 'Resource': '/test1/sUntitled 2.csv'}, code=404")
    assert expected_result == str(excinfo.value)
    s3_client_mock.get_object.assert_called_once_with(
        Bucket=SomeStuff_repository.bucket,
        Key=object_name
    )


async def test_get_some_stuff_metadata_success(SomeStuff_repository, s3_response_mock):
    object_name = 'some_stuff.png'
    s3_client_mock = SomeStuff_repository.s3_client
    s3_client_mock.head_object = s3_response_mock

    result = await SomeStuff_repository.get_some_stuff_metadata(object_name)

    assert result == {'ResponseMetadata': {'HTTPStatusCode': 200}}

    s3_client_mock.head_object.assert_called_once_with(
        Bucket=SomeStuff_repository.bucket,
        Key=object_name
    )


@pytest.mark.parametrize(
    'response_data, expected_result',
    (
            (
                {'ResponseMetadata': {'HTTPStatusCode': 403}},
                "Get some_stuff metadata object_name='some_stuff.png' failed. Response code: 403",
            ),
            (
                {},
                "Unexpected response for request_kwargs={'Key': 'some_stuff.png'}, command='head_object'",
            ),
    )
)
async def test_get_some_stuff_metadata_failes(SomeStuff_repository,
                                        s3_response_mock,
                                        response_data,
                                        expected_result):
    object_name = 'some_stuff.png'
    s3_client_mock = SomeStuff_repository.s3_client
    s3_client_mock.head_object = s3_response_mock

    async def return_response():
        if isinstance(response_data, Exception):
            raise response_data
        return response_data

    s3_response_mock.return_value = return_response()

    with pytest.raises(SomeStuffRepositoryError) as excinfo:
        await SomeStuff_repository.get_some_stuff_metadata(object_name)
    assert expected_result in str(excinfo.value)
    s3_client_mock.head_object.assert_called_once_with(
        Bucket=SomeStuff_repository.bucket,
        Key=object_name
    )


async def test_get_some_stuff_metadata_no_such_key(SomeStuff_repository, s3_response_mock):
    object_name = 'some_stuff.png'
    s3_client_mock = SomeStuff_repository.s3_client
    s3_client_mock.head_object = s3_response_mock
    s3_response_mock.side_effect = ClientError(
        {
            'Error': {'Code': '404', 'Message': 'Not Found',
                      'BucketName': 'test1', 'Resource': '/test1/sUntitled 2.csv'},
            'ResponseMetadata': {'HTTPStatusCode': 404}
        },
        'HeadObject'
    )
    with pytest.raises(SomeStuffRepositoryError) as excinfo:
        await SomeStuff_repository.get_some_stuff_metadata(object_name)
    expected_result = ("request_kwargs={'Key': 'some_stuff.png'}, command='head_object'. Error error={'Code': '404', "
                       "'Message': 'Not Found', 'BucketName': 'test1', 'Resource': '/test1/sUntitled 2.csv'}, code=404")
    assert expected_result == str(excinfo.value)
    s3_client_mock.head_object.assert_called_once_with(
        Bucket=SomeStuff_repository.bucket,
        Key=object_name
    )


async def test_remove_some_stuff_success(SomeStuff_repository, s3_response_mock):
    object_name = 'some_stuff.png'
    s3_client_mock = SomeStuff_repository.s3_client
    s3_client_mock.delete_object = s3_response_mock

    async def return_response(*args, **kwargs):
        return {'ResponseMetadata': {'HTTPStatusCode': 204}}

    s3_response_mock.return_value = return_response()

    result = await SomeStuff_repository.remove_some_stuff(object_name)

    assert result is True
    s3_client_mock.delete_object.assert_called_once_with(
        Bucket=SomeStuff_repository.bucket,
        Key=object_name
    )


@pytest.mark.parametrize(
    'response_data, expected_result',
    (
            (
                {'ResponseMetadata': {'HTTPStatusCode': 403}},
                "Deletion of some_stuff object_name='some_stuff.png' failed. Response code: 403",
            ),
            (
                {},
                "Unexpected response for request_kwargs={'Key': 'some_stuff.png'}, command='delete_object'",
            ),
    )
)
async def test_remove_some_stuff_failes(SomeStuff_repository,
                                  s3_response_mock,
                                  response_data,
                                  expected_result):
    object_name = 'some_stuff.png'
    s3_client_mock = SomeStuff_repository.s3_client
    s3_client_mock.delete_object = s3_response_mock

    async def return_response():
        if isinstance(response_data, Exception):
            raise response_data
        return response_data

    s3_response_mock.return_value = return_response()

    with pytest.raises(SomeStuffRepositoryError) as excinfo:
        await SomeStuff_repository.remove_some_stuff(object_name)

    assert expected_result in str(excinfo.value)
    s3_client_mock.delete_object.assert_called_once_with(
        Bucket=SomeStuff_repository.bucket,
        Key=object_name
    )
